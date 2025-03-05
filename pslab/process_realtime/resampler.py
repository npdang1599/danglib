import pandas as pd
import numpy as np

from danglib.pslab.resources import Globs
from danglib.pslab.logger_module import DataLogger
logger = DataLogger("/home/ubuntu/Dang/project_ps/logs", file_prefix="processdata", prefix="RESAMPLER")


class Resampler:
    """
    Handles resampling of tick data into OHLC candlestick format.
    
    This class manages the conversion of raw tick-by-tick market data into
    time-based candlestick data of specified timeframes, handling nuances of
    trading sessions, time filtering, and proper timestamp alignment.
    """
    
    # Constants for trading session time calculations (in seconds since midnight)
    TIME_CONSTANTS = {
        'MORNING': {
            'START': 9 * 3600 + 15 * 60,    # 9:15 AM
            'END': 11 * 3600 + 30 * 60      # 11:30 AM
        },
        'AFTERNOON': {
            'START': 13 * 3600,             # 1:00 PM
            'END': 14 * 3600 + 30 * 60      # 2:30 PM
        },
        'ATC': 14 * 3600 + 45 * 60         # 2:45 PM (After Trading Close)
    }

    @classmethod
    def _create_time_filter(cls, df: pd.DataFrame, time_range: dict) -> pd.Series:
        """
        Create time filter for a specific trading session.
        
        Args:
            df (pd.DataFrame): DataFrame with 'time' column (seconds since midnight)
            time_range (dict): Dictionary with 'START' and optional 'END' keys
            
        Returns:
            pd.Series: Boolean mask for filtering data within the time range
        """
        start_time = time_range['START']
        end_time = time_range.get('END', float('inf'))
        
        return (df['time'] >= start_time) & (df['time'] < end_time)
    
    @staticmethod
    def calculate_current_candletime(timestamp: float, timeframe: str, unit='s') -> float:
        """
        Calculate the start time of the current candle.
        
        Args:
            timestamp (float): Timestamp in seconds
            timeframe (str): Timeframe code (e.g., '1M', '5M', '1H')
            unit (str, optional): Time unit ('s' for seconds). Defaults to 's'.
            
        Returns:
            float: Timestamp of the current candle's start time
        """
        # Get timeframe in seconds
        tf_seconds = Globs.TF_TO_MIN.get(timeframe, 1) * 60
        
        # Round down to nearest timeframe interval
        return (timestamp // tf_seconds) * tf_seconds
    
    @staticmethod
    def calculate_finished_candletime(timestamp: float, timeframe: str, unit='s') -> float:
        """
        Calculate the start time of the previous (completed) candle.
        
        Args:
            timestamp (float): Timestamp in seconds
            timeframe (str): Timeframe code (e.g., '1M', '5M', '1H')
            unit (str, optional): Time unit ('s' for seconds). Defaults to 's'.
            
        Returns:
            float: Timestamp of the previous candle's start time
        """
        # Get current candle time
        current_candletime = Resampler.calculate_current_candletime(timestamp, timeframe)
        
        # Subtract one timeframe interval
        tf_seconds = Globs.TF_TO_MIN.get(timeframe, 1) * 60
        return current_candletime - tf_seconds

    @classmethod
    def cleanup_timestamp(cls, day: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter data to include only valid trading session times and
        standardize ATC (After Trading Close) timestamps.
        
        Args:
            day (str): Trading date in format 'YYYY_MM_DD'
            df (pd.DataFrame): DataFrame with market data
            
        Returns:
            pd.DataFrame: Filtered DataFrame with standardized timestamps
        """
        if df.empty:
            logger.log('WARNING', "Empty DataFrame in cleanup_timestamp")
            return df
            
        # Make a copy to avoid modifying the original
        df = df.copy()
        
        # Get time constants for readability
        tc = cls.TIME_CONSTANTS
        
        # Create filters for different trading sessions
        try:
            morning_filter = cls._create_time_filter(df, tc['MORNING'])
            afternoon_filter = cls._create_time_filter(df, tc['AFTERNOON'])
            atc_filter = df['time'] >= tc['ATC']
            
            # Combine filters to include only valid trading times
            valid_time_filter = morning_filter | afternoon_filter | atc_filter
            
            # Apply filter
            filtered_df = df[valid_time_filter].copy()
            
            # Standardize ATC time
            if atc_filter.any():
                # Set all ATC timestamps to the exact ATC time
                filtered_df.loc[atc_filter, 'time'] = tc['ATC']
                
                # Calculate absolute timestamp for ATC
                atc_timestamp = int(pd.to_datetime(
                    f"{day} 14:45:00", 
                    format='%Y_%m_%d %H:%M:%S'
                ).timestamp())
                
                filtered_df.loc[atc_filter, 'timestamp'] = atc_timestamp
            
            return filtered_df
            
        except Exception as e:
            logger.log('ERROR', f"Error in cleanup_timestamp: {str(e)}")
            # Return original data on error to maintain continuity
            return df

    @classmethod
    def _calculate_candletime(cls, timestamps: pd.Series, timeframe: str) -> pd.Series:
        """
        Calculate standardized candle times for the given timestamps.
        
        This method aligns timestamps to proper candle boundaries, handling
        trading session breaks and ensuring ATC data is properly assigned.
        
        Args:
            timestamps (pd.Series): Series of timestamps in seconds
            timeframe (str): Timeframe code (e.g., '1M', '5M', '1H')
            
        Returns:
            pd.Series: Series of standardized candle start times
        """
        # Get time constants for readability
        tc = cls.TIME_CONSTANTS
        
        # Calculate base timestamps
        day_start = timestamps // 86400 * 86400  # Start of day (midnight)
        seconds_in_day = timestamps % 86400      # Seconds since midnight
        
        # Key session start times
        morning_start = day_start + tc['MORNING']['START']  # 9:15 AM
        afternoon_start = day_start + tc['AFTERNOON']['START']  # 1:00 PM
        
        # Function to calculate resampled timestamps
        def _resample(ts_series: pd.Series, timeframe: str) -> pd.Series:
            """Calculate base candle times aligned to trading start time."""
            # Start from market open (9:00 AM)
            ts_market_open = day_start + 9 * 3600  
            
            # Get timeframe in seconds
            tf_seconds = Globs.TF_TO_MIN.get(timeframe) * 60
            
            # Calculate candle boundaries relative to market open
            offset_from_open = ts_series - ts_market_open
            candles_since_open = offset_from_open // tf_seconds
            
            # Calculate absolute candle times
            return candles_since_open * tf_seconds + ts_market_open
        
        # Calculate base candle times
        new_candles = _resample(timestamps, timeframe)
        new_candles_time = new_candles % 86400  # Time of day in seconds
        
        # Apply trading session adjustments
        conditions = [
            # Before morning session starts
            (new_candles_time < tc['MORNING']['START']),
            
            # Between morning and afternoon sessions
            (new_candles_time.between(
                tc['MORNING']['END'], 
                tc['AFTERNOON']['START']
            ))
        ]
        
        choices = [
            morning_start,     # Assign to morning session start
            afternoon_start    # Assign to afternoon session start
        ]
        
        # Apply adjustments using numpy.select
        adjusted_candles = pd.Series(
            np.select(conditions, choices, new_candles),
            index=timestamps.index
        )
        
        # Handle ATC specially for non-daily timeframes
        if timeframe != '1D':
            # Keep original timestamp for ATC points
            adjusted_candles = pd.Series(
                np.where(
                    seconds_in_day == tc['ATC'],
                    timestamps,
                    adjusted_candles
                ),
                index=timestamps.index
            )
        
        return adjusted_candles
    
    @classmethod
    def transform(cls, day: str, df: pd.DataFrame, timeframe: str, 
                 agg_dic: dict, cleanup_data: bool = False, subgroup=None) -> pd.DataFrame:
        """
        Transform tick-by-tick data into OHLC candlestick data.
        
        Args:
            day (str): Trading date in format 'YYYY_MM_DD'
            df (pd.DataFrame): DataFrame with market data
            timeframe (str): Timeframe code (e.g., '1M', '5M', '1H')
            agg_dic (dict): Aggregation dictionary for resampling
            cleanup_data (bool, optional): Whether to clean up timestamps. Defaults to False.
            subgroup (str, optional): Column name to group by (e.g., 'stock'). Defaults to None.
            
        Returns:
            pd.DataFrame: Resampled OHLC data
            
        Raises:
            ValueError: If required columns are missing or transformation fails
        """
        try:
            if df.empty:
                logger.log('WARNING', "Empty DataFrame in transform")
                return pd.DataFrame()
                
            # Make a copy to avoid modifying the original
            df = df.copy()
            
            # Verify required columns
            required_columns = ['timestamp']
            if subgroup:
                required_columns.append(subgroup)
                
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Required columns missing for transform: {missing_columns}")
            
            # Clean up timestamps if requested
            if cleanup_data:
                df = cls.cleanup_timestamp(day, df)
                if df.empty:
                    logger.log('WARNING', "No data left after timestamp cleanup")
                    return pd.DataFrame()
            
            # Calculate candle times
            df['candleTime'] = cls._calculate_candletime(df['timestamp'], timeframe)
            
            # Define grouping columns
            groups = ['candleTime']
            if subgroup:
                groups.append(subgroup)
            
            # Perform groupby and aggregation
            resampled_df = df.groupby(by=groups).agg(agg_dic).reset_index()
            
            # Drop timestamp column if present (it will be recreated in pivot)
            if 'timestamp' in resampled_df.columns:
                resampled_df = resampled_df.drop('timestamp', axis=1)
            
            # Create pivot table if subgroup is specified
            if subgroup:
                # Pivot with 'candleTime' as index and 'subgroup' as columns
                resampled_df = resampled_df.pivot(index='candleTime', columns=subgroup)
            else:
                # Just set 'candleTime' as index
                resampled_df = resampled_df.set_index('candleTime')
            
            return resampled_df
            
        except Exception as e:
            error_msg = f"Error in transform: {str(e)}"
            logger.log('ERROR', error_msg)
            raise ValueError(error_msg) from e