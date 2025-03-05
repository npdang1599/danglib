import pandas as pd
import numpy as np

from danglib.utils import get_ps_ticker
from danglib.pslab.utils import Utils
from danglib.pslab.resources import Adapters
from danglib.pslab.process_realtime.abstract_classes import ProcessData
from danglib.pslab.logger_module import DataLogger

# Initialize logger
logger = DataLogger("/home/ubuntu/Dang/project_ps/logs", file_prefix="processdata", prefix='PS')

class ProcessPsData(ProcessData):
    """
    Process futures/derivatives (PS) market data.
    
    This class implements the ProcessData interface specifically for futures
    and derivatives data, handling F1 contracts and related calculations.
    The class processes timestamp synchronization, OHLC calculations, and
    foreign transaction metrics for futures contracts.
    """
    
    NAME = 'PS'
    SUBGROUP = None
    
    # Constants for data normalization and processing
    GMT_OFFSET_HOURS = 7
    SECONDS_PER_DAY = 86400
    
    # Required columns for raw and processed data
    REQUIRED_RAW_COLUMNS = [
        'id', 'time', 'lastPrice', 'lastVol', 'buyForeignQtty', 
        'sellForeignQtty', 'totalMatchValue', 'totalMatchVol'
    ]
    
    REQUIRED_OUTPUT_COLUMNS = [
        'canldleTime', 'F1Open', 'F1High', 'F1Low', 'F1Close', 'F1Volume', 'F1Value', 
        'fF1BuyVol', 'fF1SellVol', 'fPsBuyVol', 'fPsSellVol', 'outstandingFPos'
    ]

    # Column mapping for standardizing column names
    REALTIME_F1_COLUMNS_MAPPING = {
        'lastPrice': 'last',
        'totalMatchValue': 'totalValue',
        'totalMatchVol': 'totalVolume',
        'buyForeignQtty': 'foreignerBuyVolume',
        'sellForeignQtty': 'foreignerSellVolume',
    }

    @classmethod
    def load_history_data(cls, from_day=None) -> pd.DataFrame:
        """
        Load historical futures/derivatives data from the plasma store.
        
        Args:
            from_day (str, optional): Starting date in format 'YYYY_MM_DD'
            
        Returns:
            pd.DataFrame: Historical futures data from plasma
            
        Raises:
            ValueError: If data loading fails
        """
        try:
            # Load market stats from plasma
            df: pd.DataFrame = Adapters.load_market_stats_from_plasma()
            
            if df.empty:
                logger.log('WARNING', "No historical futures data found in plasma store")
                return df

            # Filter by date if specified
            if from_day is not None:
                from_stamp = Utils.day_to_timestamp(from_day)
                df = df[df.index >= from_stamp]
            
            # Convert index to seconds
            df.index = df.index // 1e9

            logger.log("INFO", f"Loaded {len(df)} futures history records from plasma starting from {from_day or 'all available history'}")

            # Return only relevant columns for F1 contract data
            required_columns = [
                'F1Value', 'F1Volume', 'outstandingFPos', 'fF1BuyVol', 'fF1SellVol', 
                'fPsBuyVol', 'fPsSellVol', 'F1Open', 'F1High', 'F1Low', 'F1Close'
            ]
            
            # Ensure all required columns exist
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.log('WARNING', f"Missing columns in historical data: {missing_columns}")
                # Add missing columns with NaN values
                for col in missing_columns:
                    df[col] = np.nan
            
            return df[required_columns]
            
        except Exception as e:
            error_msg = f"Failed to load historical futures data: {str(e)}"
            logger.log("ERROR", error_msg)
            raise ValueError(error_msg) from e
    
    @classmethod
    def load_realtime_data(cls, redis_client, day, start, end) -> pd.DataFrame:
        """
        Load realtime futures/derivatives data from Redis for the specified range.
        
        Args:
            redis_client: Redis client connection
            day (str): Date in format 'YYYY_MM_DD'
            start (int): Starting position in Redis list
            end (int): Ending position in Redis list (-1 for until end)
            
        Returns:
            pd.DataFrame: Raw realtime futures data
            
        Raises:
            ValueError: If required columns are missing or data loading fails
        """
        try:
            # Load PS data from Redis
            df = Adapters.RedisAdapters.load_realtime_PS_data_from_redis(redis_client, day, start, end)
            
            if df.empty:
                logger.log('INFO', f"No new futures data found in Redis for {day} from position {start}")
                return pd.DataFrame()
            
            # Map contract codes to IDs
            try:
                ticker_map = get_ps_ticker(day)
                map_code = {k: v for v, k in ticker_map.items()}
                df['id'] = df['code'].map(map_code)
            except Exception as e:
                logger.log('ERROR', f"Error mapping futures contract codes: {str(e)}")
                return pd.DataFrame()

            # Extract only required columns
            df = df[cls.REQUIRED_RAW_COLUMNS].copy()
            
            # Validate required columns
            missing_cols = [col for col in cls.REQUIRED_RAW_COLUMNS if col not in df.columns]
            if missing_cols:
                error_msg = f"Missing columns in futures input data: {missing_cols}"
                logger.log('ERROR', error_msg)
                return pd.DataFrame()

            # Rename columns to standardized names
            df = df.rename(columns=cls.REALTIME_F1_COLUMNS_MAPPING)
            
            # Add day and process timestamps
            df['day'] = day
            
            # Convert time format (assuming time is in HHMMSS format)
            df['time'] = df['time'].astype(str).str[-6:]
            
            # Create timestamp from day and time
            df['timestamp'] = pd.to_datetime(
                day + ' ' + df['time'], 
                format='%Y_%m_%d %H%M%S'
            ).astype(int) // 1e9
            
            # Calculate seconds since midnight
            df['time'] = df['timestamp'] % cls.SECONDS_PER_DAY
            
            logger.log('INFO', f"Loaded {len(df)} realtime futures records for {day}")
            
            return df
            
        except Exception as e:
            error_msg = f"Error in load_realtime_futures_data: {str(e)}"
            logger.log('ERROR', error_msg)
            return pd.DataFrame()
    
    @classmethod
    def preprocess_realtime_data(cls, df: pd.DataFrame, day) -> pd.DataFrame:
        """
        Preprocess loaded realtime futures data before resampling.
        
        This method:
        1. Processes F1 contract data
        2. Calculates OHLC values for F1 contracts
        3. Calculates foreign buy/sell volumes
        
        Args:
            df (pd.DataFrame): Raw data loaded from Redis
            day (str): Date in format 'YYYY_MM_DD'
            
        Returns:
            pd.DataFrame: Preprocessed data ready for resampling
            
        Raises:
            ValueError: If preprocessing fails
        """
        try:
            if df.empty:
                logger.log('WARNING', f"Empty input dataframe in {cls.NAME} preprocessing")
                return pd.DataFrame()
                
            # Make a copy to avoid modifying the original
            df = df.copy()
            
            # Sort by cumulative values for accurate diff calculation
            df.sort_values(['foreignerBuyVolume', 'foreignerSellVolume', 'time', 'day'], inplace=True)
            
            # Process F1 contract data
            f1_data = cls._process_f1_contract_data(df)
            
            # Return only required columns with correct order
            required_cols = [
                'time', 'timestamp', 'F1Open', 'F1High', 'F1Low', 'F1Close', 
                'F1Volume', 'F1Value', 'fF1BuyVol', 'fF1SellVol', 
                'fPsBuyVol', 'fPsSellVol', 'outstandingFPos'
            ]
            
            # Ensure all required columns exist
            for col in required_cols:
                if col not in f1_data.columns:
                    logger.log('WARNING', f"Required column {col} missing in preprocessed futures data")
                    if col in ['fPsBuyVol', 'fPsSellVol', 'outstandingFPos']:
                        f1_data[col] = 0  # Default values for PS-specific columns
            
            return f1_data[required_cols]
            
        except Exception as e:
            error_msg = f"Error in preprocess_realtime_futures_data: {str(e)}"
            logger.log('ERROR', error_msg)
            raise ValueError(error_msg) from e
    
    @classmethod
    def _process_f1_contract_data(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process F1 futures contract data.
        
        Args:
            df (pd.DataFrame): Raw futures data
            
        Returns:
            pd.DataFrame: Processed F1 contract data
        """
        # Extract only F1 contract data
        f1_data = df[df['id'] == 'f1'].copy()
        
        if f1_data.empty:
            logger.log('WARNING', "No F1 contract data found")
            return pd.DataFrame()
            
        # Sort by total volume for accurate diff calculation
        f1_data = f1_data.sort_values('totalVolume')
        
        # Set OHLC values (initially all equal to last price)
        f1_data['F1Open'] = f1_data['F1High'] = f1_data['F1Low'] = f1_data['F1Close'] = f1_data['last'].copy()
        
        # Calculate volume and value differences
        f1_data['F1Volume'] = f1_data['totalVolume'].diff()
        f1_data['F1Value'] = f1_data['totalValue'].diff()
        
        # Calculate foreign buy/sell volume differences
        f1_data['fF1BuyVol'] = f1_data['foreignerBuyVolume'].diff().fillna(f1_data['foreignerBuyVolume'])
        f1_data['fF1SellVol'] = f1_data['foreignerSellVolume'].diff().fillna(f1_data['foreignerSellVolume'])
        
        # Initialize additional required fields
        f1_data['totalBidVolume'] = 0
        f1_data['totalAskVolume'] = 0
        f1_data['fPsBuyVol'] = 0
        f1_data['fPsSellVol'] = 0
        f1_data['outstandingFPos'] = 0
        
        return f1_data
    
    @classmethod
    def postprocess_resampled_data(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Postprocess data after resampling to ensure data integrity.
        
        This method fills missing values for F1 contract data.
        
        Args:
            df (pd.DataFrame): Resampled futures data
            
        Returns:
            pd.DataFrame: Processed data with missing values filled appropriately
        """
        if df.empty:
            logger.log('WARNING', "Empty DataFrame in postprocess_resampled_data for futures")
            return df
            
        # Make a copy to avoid modifying the original
        df = df.copy()
        
        # Fill foreign transaction volumes with zeros
        df[['fF1BuyVol', 'fF1SellVol']] = df[['fF1BuyVol', 'fF1SellVol']].fillna(0)
        
        # Handle OHLC values
        df['F1Close'] = df['F1Close'].ffill().bfill()
        df['F1Open'] = df['F1Open'].fillna(df['F1Close'])
        df['F1High'] = df['F1High'].fillna(df['F1Close'])
        df['F1Low'] = df['F1Low'].fillna(df['F1Close'])
        
        # Fill numerical metrics with zeros
        df['F1Volume'] = df['F1Volume'].fillna(0)
        df['F1Value'] = df['F1Value'].fillna(0)
        df['fPsBuyVol'] = df['fPsBuyVol'].fillna(0)
        df['fPsSellVol'] = df['fPsSellVol'].fillna(0)
        df['outstandingFPos'] = df['outstandingFPos'].fillna(0)

        return df
    
    @classmethod
    def get_last_states(cls, df: pd.DataFrame, seconds=None, num_dps=None) -> pd.DataFrame:
        """
        Get last states of futures data for maintaining continuity between processing cycles.
        
        Either `seconds` or `num_dps` should be provided, but not both.
        
        Args:
            df (pd.DataFrame): Dataset to extract last states from
            seconds (int, optional): Number of seconds of data to include
            num_dps (int, optional): Number of data points to include
            
        Returns:
            pd.DataFrame: Subset of data representing the last states
        """
        try:
            if df.empty:
                logger.log('WARNING', "Empty DataFrame in get_last_states for futures")
                return pd.DataFrame()
                
            # Verify timestamp column exists
            if 'timestamp' not in df.columns:
                logger.log('ERROR', "Required column 'timestamp' missing from futures DataFrame")
                return pd.DataFrame()
            
            # Filter by time window
            if seconds is not None:
                timestamp = df['timestamp'].max()
                from_timestamp = timestamp - seconds
                
                # Return data within time window
                return df[df['timestamp'] >= from_timestamp].copy()
            
            # Filter by number of data points
            elif num_dps is not None:
                if num_dps <= 0:
                    logger.log('WARNING', f"Invalid num_dps value: {num_dps}, using default of 1")
                    num_dps = 1
                
                # Group by ID if present, otherwise return last N rows
                if 'id' in df.columns:
                    return df.groupby('id').tail(num_dps)
                else:
                    return df.tail(num_dps)
            
            else:
                # If neither parameter is provided, return empty DataFrame
                logger.log('WARNING', "Neither seconds nor num_dps provided to get_last_states for futures")
                return pd.DataFrame()
                
        except Exception as e:
            logger.log('ERROR', f"Error in get_last_states for futures: {str(e)}")
            return pd.DataFrame()
    
    @classmethod
    def get_agg_dic(cls, stats=None) -> dict:
        """
        Get aggregation dictionary for resampling operations.
        
        Args:
            stats (list, optional): List of columns to include in aggregation
            
        Returns:
            dict: Mapping of column names to aggregation functions
        """
        if stats is None:
            stats = cls.REQUIRED_OUTPUT_COLUMNS
            
        # Default aggregation dictionary
        agg_dict = {
            'F1Value': 'sum',
            'F1Volume': 'sum',
            'outstandingFPos': 'last',
            'fPsBuyVol': 'sum',
            'fPsSellVol': 'sum',
            'fF1BuyVol': 'sum',
            'fF1SellVol': 'sum',
            'F1Open': 'first',
            'F1High': 'max',
            'F1Low': 'min',
            'F1Close': 'last',
            'time': 'last'
        }
        
        # Filter to include only requested stats
        return {k: v for k, v in agg_dict.items() if k in stats}