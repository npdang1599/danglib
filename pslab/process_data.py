import pandas as pd
import numpy as np
from redis import StrictRedis
from datetime import datetime
import time
from abc import ABC, abstractmethod

from danglib.utils import check_run_with_interactive, totime
from danglib.pslab.utils import Utils
from danglib.pslab.resources import Adapters, Globs
from danglib.lazy_core import gen_plasma_functions
from danglib.pslab.logger_module import DataLogger
from danglib.utils import get_ps_ticker

# Initialize Redis clients
r = StrictRedis('ws2', decode_responses=True)
rlv2 = StrictRedis(host='lv2', decode_responses=True)

# Initialize logger
logger = DataLogger("/home/ubuntu/Dang/project_ps/logs", file_prefix="processdata")

class ProcessData(ABC):
    """
    Abstract base class for financial data processing.
    
    This class defines the interface for processing different types of financial data,
    such as stock market data or futures/derivatives data. Subclasses must implement
    all abstract methods to provide specific data processing logic.
    """
    
    SUBGROUP = None
    
    @classmethod
    @abstractmethod
    def load_history_data(cls, from_day=None) -> pd.DataFrame:
        """
        Load historical data from a data source.
        
        Args:
            from_day (str, optional): Starting date in format 'YYYY_MM_DD'. 
                                     If None, loads all available history.
                                     
        Returns:
            pd.DataFrame: Historical data with appropriate structure
        """
        pass
    
    @classmethod
    @abstractmethod
    def load_realtime_data(cls, redis_client, day, start, end) -> pd.DataFrame:
        """
        Load realtime data from Redis for the specified range.
        
        Args:
            redis_client: Redis client connection
            day (str): Date in format 'YYYY_MM_DD'
            start (int): Starting position in Redis list
            end (int): Ending position in Redis list (-1 for until end)
            
        Returns:
            pd.DataFrame: Raw realtime data
        """
        pass

    @classmethod
    @abstractmethod
    def preprocess_realtime_data(cls, df: pd.DataFrame, day) -> pd.DataFrame:
        """
        Preprocess loaded realtime data before resampling.
        
        Args:
            df (pd.DataFrame): Raw data loaded from Redis
            day (str): Date in format 'YYYY_MM_DD'
            
        Returns:
            pd.DataFrame: Preprocessed data ready for resampling
        """
        pass

    @classmethod
    @abstractmethod
    def postprocess_resampled_data(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Postprocess data after resampling to ensure data integrity.
        
        Args:
            df (pd.DataFrame): Resampled data
            
        Returns:
            pd.DataFrame: Processed data with missing values filled and corrections applied
        """
        pass

    @classmethod
    @abstractmethod
    def get_agg_dic(cls) -> dict:
        """
        Get aggregation dictionary for resampling operations.
        
        Returns:
            dict: Map of column names to aggregation functions
        """
        pass

    @classmethod
    @abstractmethod
    def get_last_states(cls, df: pd.DataFrame, seconds=None, num_dps=None) -> pd.DataFrame:
        """
        Get last states of data for maintaining continuity between processing cycles.
        
        Args:
            df (pd.DataFrame): Dataset to extract last states from
            seconds (int, optional): Number of seconds of data to include
            num_dps (int, optional): Number of data points to include
            
        Returns:
            pd.DataFrame: Subset of data representing the last states
        """
        pass

    @classmethod
    def process_signals(cls):
        """
        Process market signals based on the resampled data.
        Default implementation does nothing.
        """
        pass

class ProcessStockData(ProcessData):
    """
    Process stock market data from various sources.
    
    This class implements the ProcessData interface specifically for stock market data,
    handling the nuances of stock data processing including foreign transactions,
    bid/ask calculations, and stock-specific aggregations.
    """
    
    # Constants for time and numerical calculations
    GMT_OFFSET_HOURS = 7
    NORMALIZATION_FACTOR = 1e9
    MERGE_ASOF_TOLERANCE_SECONDS = 15
    MAX_CONCURRENT_TASKS = 5
    END_OF_TRADING_SECONDS = 53159  # 14:45:59 in seconds
    
    # Column mappings for realtime HOSE500 data
    REALTIME_HOSE500_COLUMNS_MAPPING = {
        'code': 'stock', 
        'lastPrice': 'close',
        'lastVol': 'volume',
        'buyForeignQtty': 'foreignerBuyVolume',
        'sellForeignQtty': 'foreignerSellVolume',
        'totalMatchVol': 'totalMatchVolume',
        'best1Bid': 'bestBid1',
        'best1BidVol': 'bestBid1Volume',
        'best2Bid': 'bestBid2',
        'best2BidVol': 'bestBid2Volume',
        'best3Bid': 'bestBid3',
        'best3BidVol': 'bestBid3Volume',
        'best1Offer': 'bestOffer1',
        'best1OfferVol': 'bestOffer1Volume',
        'best2Offer': 'bestOffer2',
        'best2OfferVol': 'bestOffer2Volume',
        'best3Offer': 'bestOffer3',
        'best3OfferVol': 'bestOffer3Volume',
    }

    # Aggregation dictionary for resampling
    RESAMPLE_AGG_DIC = {
        'open': 'first', 
        'high': 'max', 
        'low': 'min', 
        'close': 'last', 
        'matchingValue': 'sum',
        'bu': 'sum', 
        'sd': 'sum',
        'bu2': 'sum',
        'sd2': 'sum', 
        'bid': 'last', 
        'ask': 'last',
        'refPrice': 'last',
        'time': 'last', 
        'fBuyVal': 'sum', 
        'fSellVal': 'sum'
    }

    # Required columns for output
    REQUIRED_COLUMNS = [
        'timestamp', 'open', 'high', 'low', 'close', 'matchingValue', 
        'bu', 'sd', 'bu2', 'sd2', 'bid', 'ask', 'refPrice',
        'stock', 'time', 'fBuyVal', 'fSellVal'
    ]

    SUBGROUP = 'stock'
    NAME = 'STOCK'

    @staticmethod
    def load_strategies():
        """
        Load trading strategies from JSON file.
        
        Returns:
            dict: Trading strategies configuration
        
        Raises:
            FileNotFoundError: If the strategy file cannot be found
            json.JSONDecodeError: If the strategy file contains invalid JSON
        """
        import json
        path = '/home/ubuntu/Dang/pslab_strategies.json'
        try:
            with open(path, 'r') as f:
                strategies = json.load(f)
            return strategies
        except FileNotFoundError:
            logger.log('ERROR', f"Strategy file not found at {path}")
            return {}
        except json.JSONDecodeError:
            logger.log('ERROR', f"Invalid JSON in strategy file at {path}")
            return {}
    
    STRATEGIES = load_strategies()

    @classmethod
    def get_agg_dic(cls):
        """
        Get aggregation dictionary for resampling operations.
        
        Returns:
            dict: Mapping of column names to aggregation functions
        """
        return cls.RESAMPLE_AGG_DIC
    
    @staticmethod
    def ensure_all_stocks_present(df: pd.DataFrame, required_stocks: list) -> pd.DataFrame:
        """
        Ensure all stocks are present in DataFrame by adding rows with NaN for missing stocks.
        
        This is important for maintaining consistent data structure and preventing
        errors in downstream processing.
        
        Args:
            df (pd.DataFrame): Original DataFrame
            required_stocks (list): List of required stock symbols
            data_name (str, optional): Name of the data for logging purposes
            
        Returns:
            pd.DataFrame: DataFrame with all required stocks included
            
        Raises:
            ValueError: If the input DataFrame doesn't have required columns
        """
        try:
            if df.empty:
                logger.log('WARNING', "Input DataFrame is empty in ensure_all_stocks_present")
                return df
            
            # Verify required columns exist
            required_columns = ['stock', 'timestamp', 'time']
            for col in required_columns:
                if col not in df.columns:
                    raise ValueError(f"Required column '{col}' missing from DataFrame")
                
            # Get maximum timestamp from existing data
            max_timestamp = df['timestamp'].max()
            max_time = df['time'].max()
            
            # Check for missing stocks
            existing_stocks = set(df['stock'].unique())
            required_stocks_set = set(required_stocks)
            missing_stocks = required_stocks_set - existing_stocks
            
            if missing_stocks:
                logger.log('WARNING', f"Missing stocks in data: {missing_stocks}")
                
                # Add missing stocks more efficiently
                if len(missing_stocks) > 0:
                    # Create DataFrame for missing stocks at once
                    missing_data = {col: [np.nan] * len(missing_stocks) for col in df.columns}
                    missing_data['stock'] = list(missing_stocks)
                    missing_data['timestamp'] = [max_timestamp] * len(missing_stocks)
                    missing_data['time'] = [max_time] * len(missing_stocks)
                    
                    df_missing = pd.DataFrame(missing_data)
                    df = pd.concat([df, df_missing], ignore_index=True)
                    
                    logger.log('INFO', f"Added {len(missing_stocks)} rows for missing stocks at {totime(max_timestamp, unit='ms')}")
            
            return df
            
        except Exception as e:
            logger.log('ERROR', f"Error in ensure_all_stocks_present: {str(e)}")
            # Re-raise with more context for better debugging
            raise ValueError(f"Failed to ensure all stocks are present: {str(e)}") from e

