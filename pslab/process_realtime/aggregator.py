import time

import pandas as pd
import numpy as np
from datetime import datetime
from redis import StrictRedis

from danglib.pslab.process_realtime.resampler import Resampler
from danglib.lazy_core import gen_plasma_functions
from danglib.pslab.logger_module import DataLogger
from danglib.pslab.process_realtime.abstract_classes import ProcessData
from danglib.pslab.resources import Globs, Adapters

logger = DataLogger("/home/ubuntu/Dang/project_ps/logs", file_prefix="processdata", prefix="AGGREGATOR")


class Aggregator:
    """
    Orchestrates real-time data aggregation and resampling.
    
    This class manages the continuous process of fetching, processing,
    and resampling real-time market data. It handles both stock and
    futures data, maintains state between processing cycles, and stores
    results in the plasma store.
    """
    
    # Trading time constants
    TRADING_HOURS = {
        'MORNING_START': datetime.strptime('09:15', '%H:%M').time(),
        'LUNCH_START': datetime.strptime('11:30', '%H:%M').time(),
        'LUNCH_END': datetime.strptime('13:00', '%H:%M').time(),
        'AFTERNOON_END': datetime.strptime('14:50', '%H:%M').time()
    }
    
    # Sleep durations (in seconds)
    SLEEP_DURATION = {
        'OUTSIDE_TRADING': 60,  # Sleep time outside trading hours
        'EMPTY_DATA': 1,        # Sleep time when no new data
        'ERROR': 1              # Sleep time after error
    }

    def __init__(self, day: str, timeframe: str, output_plasma_key: str, data_processor: ProcessData, redis_cli: StrictRedis = None):
        """
        Initialize the Aggregator.
        
        Args:
            day (str): Trading date in format 'YYYY_MM_DD'
            timeframe (str): Timeframe code (e.g., '1M', '5M', '1H')
            output_plasma_key (str): Key for storing results in plasma store
            data_processor (ProcessData): Data processor implementation to use
        """
        self.r = redis_cli
        self.day = day
        self.timeframe = timeframe
        self.timeframe_seconds = Globs.TF_TO_MIN.get(timeframe) * 60
        self.output_key = f"{output_plasma_key}.{timeframe}"
        self.data_processor = data_processor

        # State variables
        self.current_pos = 0  # Current position in Redis list
        self.current_candle_stamp = 0  # Current candle timestamp
        self.resampled_df = pd.DataFrame()  # Resampled data
        self.last_states = pd.DataFrame()  # Last states for continuity
        
        logger.log('INFO', 
                 f"Initialized Aggregator for {day} "
                 f"with timeframe {timeframe}, output key: {self.output_key}")
        
    def join_historical_data(self, df: pd.DataFrame, ndays: int = 10) -> pd.DataFrame:
        """
        Join current data with historical data for context.
        
        This method loads historical data from the plasma store and
        combines it with the current data to provide context for
        calculations that require historical data points.
        
        Args:
            df (pd.DataFrame): Current resampled data
            ndays (int, optional): Number of days of history to include. Defaults to 10.
            
        Returns:
            pd.DataFrame: Combined historical and current data
        """
        try:
            # Get starting day for historical data
            from_day = Adapters.get_historical_day_by_index(-ndays)
            logger.log('INFO', f'Merging historical data from {from_day}...')

            # Load historical data
            df_history = self.data_processor.load_history_data(from_day)
            
            if df_history.empty:
                logger.log('WARNING', f"No historical data available from {from_day}")
                return df
            
            # Merge historical and current data
            df_merged = pd.concat([df_history, df])
            df_merged = df_merged.sort_index(ascending=True)

            logger.log('INFO', 
                     f'Successfully merged historical data. '
                     f'Before: {df.shape}, After: {df_merged.shape}')
                     
            return df_merged
            
        except Exception as e:
            logger.log('ERROR', f"Error joining historical data: {str(e)}")
            # Return original data on error to maintain continuity
            return df
    
    def _get_next_candle_stamp(self, current_timestamp: float) -> float:
        """
        Calculate the start timestamp of the next candle.
        
        Args:
            current_timestamp (float): Current timestamp in seconds
            
        Returns:
            float: Start timestamp of the next candle
        """
        current_candle = self._get_candle_stamp(current_timestamp)
        return current_candle + self.timeframe_seconds
    
    def _get_candle_stamp(self, timestamp: float) -> float:
        """
        Calculate the start timestamp of the current candle.
        
        Args:
            timestamp (float): Timestamp in seconds
            
        Returns:
            float: Start timestamp of the current candle
        """
        return (timestamp // self.timeframe_seconds) * self.timeframe_seconds
    
    def _add_empty_candle(self, candle_timestamp: float, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add an empty candle with NaN values to the DataFrame.
        
        This is used when no data is available for a time period but
        continuity in the series is needed.
        
        Args:
            candle_timestamp (float): Timestamp for the empty candle
            df (pd.DataFrame): DataFrame to add the empty candle to
            
        Returns:
            pd.DataFrame: DataFrame with empty candle added
        """
        try:
            if df.empty:
                logger.log('WARNING', "Cannot add empty candle to empty DataFrame")
                return df
                
            # Create a DataFrame with a single row for the empty candle
            new_df = pd.DataFrame(
                index=[candle_timestamp],
                columns=df.columns,
                data=np.nan
            )
            
            # Concat with original DataFrame
            result_df = pd.concat([df, new_df])
            
            logger.log('INFO', f"Added empty candle at {candle_timestamp} "
                            f"({datetime.fromtimestamp(candle_timestamp).strftime('%H:%M:%S')})")
            
            return result_df
            
        except Exception as e:
            logger.log('ERROR', f"Error adding empty candle: {str(e)}")
            return df
    
    def _update_last_states(self, df: pd.DataFrame, seconds: int = 30) -> None:
        """
        Update the last states from the current DataFrame.
        
        This method extracts recent data to maintain state between
        processing cycles, which is crucial for accurate differential calculations.
        
        Args:
            df (pd.DataFrame): Current DataFrame to extract last states from
            seconds (int, optional): Number of seconds of data to include. Defaults to 30.
        """
        try:
            if df.empty:
                logger.log('WARNING', "Cannot update last states from empty DataFrame")
                return
                
            # Get last states using the data processor
            self.last_states = self.data_processor.get_last_states(df, seconds=seconds)
            
            # Log state update
            if not self.last_states.empty:
                processor_name = self.data_processor.NAME
                num_records = len(self.last_states)
                
                if hasattr(self.data_processor, 'SUBGROUP') and self.data_processor.SUBGROUP:
                    # For grouped data (e.g., stocks)
                    subgroup_col = self.data_processor.SUBGROUP
                    if subgroup_col in self.last_states.columns:
                        num_groups = self.last_states[subgroup_col].nunique()
                        logger.log('INFO', 
                                f"Updated {processor_name} last states: {num_records} records "
                                f"for {num_groups} {subgroup_col}s")
                else:
                    # For non-grouped data
                    logger.log('INFO', f"Updated {processor_name} last states: {num_records} records")
                    
        except Exception as e:
            logger.log('ERROR', f"Error updating last states: {str(e)}")
    
    def _append_last_states(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Append last states to the beginning of a new DataFrame.
        
        This is critical for maintaining continuity in differential calculations.
        
        Args:
            df (pd.DataFrame): New DataFrame to append last states to
            
        Returns:
            pd.DataFrame: Combined DataFrame with last states at the beginning
        """
        try:
            if df.empty:
                logger.log('WARNING', "New DataFrame is empty in _append_last_states")
                return df
                
            if self.last_states.empty:
                logger.log('INFO', "No last states to append")
                return df
                
            # Combine last states with new data
            combined_df = pd.concat([self.last_states, df])
            
            logger.log('INFO', 
                    f"Appended {len(self.last_states)} last state records to {len(df)} new records")
                    
            return combined_df
            
        except Exception as e:
            logger.log('ERROR', f"Error appending last states: {str(e)}")
            return df
        
    def load_realtime_resampled_data(self) -> pd.DataFrame:
        """
        Load previously resampled data from the plasma store.
        
        Returns:
            pd.DataFrame: Previously resampled data, or empty DataFrame if none exists
        """
        try:
            # Get plasma store functions
            _, disconnect, _, pload = gen_plasma_functions(db=Globs.PLASMA_DB)
            
            # Load data from plasma
            df = pload(self.output_key)
            
            if df is not None and not df.empty:
                logger.log('INFO', 
                        f"Loaded existing resampled data from plasma: {self.output_key} "
                        f"with shape {df.shape}")
            else:
                logger.log('INFO', f"No existing resampled data found in plasma: {self.output_key}")
                df = pd.DataFrame()
                
            return df
            
        except Exception as e:
            logger.log('ERROR', f"Error loading resampled data from plasma: {str(e)}")
            return pd.DataFrame()

    @staticmethod
    def merge_resampled_data(old_df: pd.DataFrame, new_df: pd.DataFrame, 
                           agg_dict: dict, subgroup=None) -> pd.DataFrame:
        """
        Merge and recalculate values for overlapping candles.
        
        This method handles merging new data with existing data, ensuring
        proper recalculation of aggregates for candles that appear in both.
        
        Args:
            old_df (pd.DataFrame): Existing resampled data
            new_df (pd.DataFrame): New resampled data
            agg_dict (dict): Aggregation dictionary for recalculating values
            subgroup (str, optional): Column to group by. Defaults to None.
            
        Returns:
            pd.DataFrame: Merged data with proper aggregations
        """
        try:
            # If old data is empty, just return new data
            if old_df.empty:
                return new_df
                
            # If new data is empty, just return old data
            if new_df.empty:
                return old_df
                
            # Find common indices (timestamps)
            common_idx = old_df.index.intersection(new_df.index)
            
            # Set column names for pivoted data
            if subgroup:
                old_df.columns.names = ['stats', subgroup]
                new_df.columns.names = ['stats', subgroup]
                
            # Handle overlapping candles
            if not common_idx.empty:
                # Combine data for overlapping candles
                overlap_data = pd.concat([
                    old_df.loc[common_idx],
                    new_df.loc[common_idx]
                ])
                
                # Recalculate aggregations for overlapping candles
                if subgroup:
                    # For pivoted data (e.g., stocks)
                    recalculated = overlap_data.stack()
                    recalculated = (recalculated.groupby(level=[0, 1])
                                   .agg(agg_dict)
                                   .unstack())
                else:
                    # For non-pivoted data
                    recalculated = overlap_data.groupby(level=0).agg(agg_dict)
                
                # Combine all parts: old-only, recalculated overlap, new-only
                result_df = pd.concat([
                    old_df.loc[old_df.index.difference(common_idx)],  # Rows only in old
                    recalculated,                                     # Recalculated overlaps
                    new_df.loc[new_df.index.difference(common_idx)]   # Rows only in new
                ]).sort_index()
                
            else:
                # If no overlap, just concatenate and sort
                result_df = pd.concat([old_df, new_df]).sort_index()
            
            logger.log('INFO', 
                    f"Merged resampled data: old({old_df.shape}) + new({new_df.shape}) "
                    f"-> result({result_df.shape})")
                    
            return result_df
            
        except Exception as e:
            logger.log('ERROR', f"Error merging resampled data: {str(e)}")
            # Return old data on error to maintain continuity
            return old_df
        

    def _store_resampled_data(self, df: pd.DataFrame) -> bool:
        """
        Store resampled data in the plasma store.
        
        Args:
            df (pd.DataFrame): Resampled data to store
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if df.empty:
                logger.log('WARNING', "Cannot store empty DataFrame to plasma")
                return False
                
            # Get plasma store functions
            _, disconnect, psave, _ = gen_plasma_functions(db=Globs.PLASMA_DB)
            
            # Store data in plasma
            psave(self.output_key, df)
            
            logger.log('INFO', f"Stored resampled data to plasma: {self.output_key} with shape {df.shape}")
            return True
            
        except Exception as e:
            logger.log('ERROR', f"Error storing resampled data to plasma: {str(e)}")
            return False
    
    def start_realtime_resampling(self) -> None:
        """
        Start the real-time resampling process.
        
        This is the main method that orchestrates the continuous process of:
        1. Loading new data from Redis
        2. Preprocessing the data
        3. Resampling into candles of the specified timeframe
        4. Storing results in the plasma store
        5. Processing signals using the data processor
        
        The process runs continuously, respecting trading hours and handling
        errors to maintain operational continuity.
        """
        logger.log('INFO', f"Starting realtime resampling with timeframe: {self.timeframe}")

        while True:
            try:
                # Get current time
                current_time = datetime.now()
                time_str = current_time.strftime('%H:%M:%S')
                current_time = current_time.time()

                # Skip if outside trading hours
                if not (self.TRADING_HOURS['MORNING_START'] <= current_time <= self.TRADING_HOURS['AFTERNOON_END']):
                    logger.log('INFO', f"Outside trading hours ({time_str})")
                    time.sleep(self.SLEEP_DURATION['OUTSIDE_TRADING'])
                    continue

                # Skip during lunch break
                if self.TRADING_HOURS['LUNCH_START'] <= current_time <= self.TRADING_HOURS['LUNCH_END']:
                    logger.log('INFO', f"Lunch break ({time_str})")
                    time.sleep(self.SLEEP_DURATION['OUTSIDE_TRADING'])
                    continue

                # Load new data from Redis
                raw_data = self.data_processor.load_realtime_data(self.r, self.day, self.current_pos, -1)
                logger.log('INFO', f"Processing data from position {self.current_pos}")
                
                # Update position for next iteration
                self.current_pos += len(raw_data) + 1
                logger.log('INFO', f"Updated current_pos to {self.current_pos}")

                # Process new data if available
                if not raw_data.empty:
                    # Append last states for continuity
                    raw_data_with_last_states = self._append_last_states(raw_data)

                    # Preprocess the raw data
                    new_data = self.data_processor.preprocess_realtime_data(
                        raw_data_with_last_states, self.day
                    )
                    
                    # Filter data newer than current candle
                    new_data = new_data[new_data['timestamp'] >= self.current_candle_stamp]

                    # Update last states for next iteration
                    self._update_last_states(raw_data_with_last_states, seconds=30)

                    if not new_data.empty:
                        # Resample the preprocessed data
                        new_resampled = Resampler.transform(
                            day=self.day,
                            df=new_data,
                            timeframe=self.timeframe,
                            agg_dic=self.data_processor.get_agg_dic(),
                            cleanup_data=True,
                            subgroup=self.data_processor.SUBGROUP
                        )
                        
                        logger.log('INFO', f"Resampled new data into shape: {new_resampled.shape}")

                        # Initialize resampled DataFrame if empty
                        if self.resampled_df.empty:
                            self.resampled_df = self.join_historical_data(new_resampled, ndays=1)
                            logger.log('INFO', f"Initialized resampled DataFrame with shape: {self.resampled_df.shape}")
                        else:
                            # Merge with existing resampled data
                            self.resampled_df = self.merge_resampled_data(
                                old_df=self.resampled_df, 
                                new_df=new_resampled,
                                agg_dict=self.data_processor.get_agg_dic(),
                                subgroup=self.data_processor.SUBGROUP
                            )
                            
                            logger.log('INFO', f"Updated resampled DataFrame to shape: {self.resampled_df.shape}")
                        
                        # Apply post-processing to fill gaps and ensure data integrity
                        self.resampled_df = self.data_processor.postprocess_resampled_data(self.resampled_df)

                        # Update current candle timestamp
                        current_candle_stamp = self._get_candle_stamp(new_data['timestamp'].max())
                        if current_candle_stamp > self.current_candle_stamp:
                            self.current_candle_stamp = current_candle_stamp
                        else:
                            # If no new candle, add an empty one for the next timeframe
                            next_candle_stamp = self._get_next_candle_stamp(self.current_candle_stamp)
                            self.current_candle_stamp = next_candle_stamp
                            self.resampled_df = self._add_empty_candle(next_candle_stamp, self.resampled_df)
                            logger.log('INFO', f"Added empty candle at {self.current_candle_stamp}")

                        logger.log('INFO', f"Updated current_candle_stamp to {self.current_candle_stamp}")
                        
                        # Store results to plasma
                        self._store_resampled_data(self.resampled_df)
                        
                        # Process signals with minimal delay
                        time.sleep(0.1)
                        self.data_processor.process_signals()

                # Calculate sleep time until next candle
                current_stamp = time.time() + 7*3600  # Adjust for GMT+7
                next_candle_time = self._get_next_candle_stamp(current_stamp)
                sleep_duration = max(0, next_candle_time - current_stamp)

                logger.log('INFO', f"Completed processing cycle, next candle at {datetime.fromtimestamp(next_candle_time).strftime('%H:%M:%S')}")
                
                # Sleep until next candle time
                if sleep_duration > 0:
                    time.sleep(sleep_duration)

            except Exception as e:
                logger.log('ERROR', f"Error in realtime resampling: {str(e)}", exc_info=True)
                time.sleep(self.SLEEP_DURATION['ERROR'])

