import pandas as pd
from abc import ABC, abstractmethod


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