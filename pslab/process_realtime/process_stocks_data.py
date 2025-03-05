import time

import pandas as pd
import numpy as np
from redis import StrictRedis

from danglib.lazy_core import gen_plasma_functions
from danglib.utils import totime
from danglib.pslab.process_realtime.abstract_classes import ProcessData
from danglib.pslab.logger_module import DataLogger
from danglib.pslab.resources import Globs, Adapters
from danglib.pslab.utils import Utils

# Initialize logger
logger = DataLogger("/home/ubuntu/Dang/project_ps/logs", file_prefix="processdata", prefix="STOCK")

rlv2 = StrictRedis(host='lv2', decode_responses=True)

class ProcessStockData(ProcessData):
    """
    Process stock market data from various sources.
    
    This class implements the ProcessData interface specifically for stock market data,
    handling the nuances of stock data processing including foreign transactions,
    bid/ask calculations, and stock-specific aggregations.
    """
    
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
        """
        import json
        path = '/home/ubuntu/Dang/pslab_strategies.json'
        with open(path, 'r') as f:
            strategies = json.load(f)
        return strategies
    
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
    def ensure_all_stocks_present(df: pd.DataFrame, required_stocks: list, data_name='') -> pd.DataFrame:
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
        """
        try:
            if df.empty:
                logger.log('WARNING', "Input DataFrame is empty in ensure_all_stocks_present")
                return df
                
            # Get maximum timestamp from existing data
            max_timestamp = df['timestamp'].max()
            max_time = df['time'].max()
            
            # Check for missing stocks
            existing_stocks = set(df['stock'].unique())
            required_stocks_set = set(required_stocks)
            missing_stocks = required_stocks_set - existing_stocks
            
            if missing_stocks:
                logger.log('WARNING', f"Missing stocks in data {data_name}: {missing_stocks}")
                
                # Create template for new rows
                template_row = pd.Series(
                    index=df.columns,
                    data=np.nan
                )
                
                # Create new rows for missing stocks
                new_rows = []
                for stock in missing_stocks:
                    new_row = template_row.copy()
                    new_row['stock'] = stock
                    new_row['timestamp'] = max_timestamp
                    new_row['time'] = max_time
                    new_rows.append(new_row)
                
                # Add new rows to DataFrame
                if new_rows:
                    df_missing = pd.DataFrame(new_rows)
                    df = pd.concat([df, df_missing], ignore_index=True)
                    logger.log('INFO', f"Added {len(new_rows)} rows for missing stocks at {totime(max_timestamp, unit='ms')}")
            
            return df
            
        except Exception as e:
            logger.log('ERROR', f"Error in ensure_all_stocks_present: {str(e)}")
            return df

    class ComputeStats:
        """
        Inner class for computing financial statistics from stock data.
        
        This class contains methods for calculating various financial metrics 
        like foreign transactions and bid/ask metrics.
        """
        
        @staticmethod
        def calculate_foreign_fubon(
            dfi: pd.DataFrame,
            put_through_data: pd.DataFrame = None,
            tolerance: float = 0.002
        ) -> pd.DataFrame:
            """
            Calculate foreign transactions for stocks with optimized performance.
            
            This method identifies foreign buy/sell transactions and filters out
            put-through transactions to get a clearer picture of market activity.
            
            Args:
                dfi (pd.DataFrame): Preprocessed dataframe containing stock data with columns:
                    - stock: stock symbol
                    - foreignerBuyVolume: cumulative foreign buy volume
                    - foreignerSellVolume: cumulative foreign sell volume
                    - time: trade time in seconds from start of day
                put_through_data (pd.DataFrame, optional): Put-through transaction data with columns:
                    - stockSymbol: stock symbol 
                    - vol: transaction volume
                    - createdAt: transaction time in HH:MM:SS format
                tolerance (float, optional): Tolerance for matching put-through volumes. Defaults to 0.002
                
            Returns:
                pd.DataFrame: Processed data with foreign transactions, excluding put-through matches
            """
            try:
                df = dfi.copy()
                logger.log('INFO', f"Processing foreign transactions for {len(df)} records")
    
                if df.empty:
                    logger.log('WARNING', "Empty input dataframe for foreign calculations")
                    return pd.DataFrame()
                
                # Validate required columns
                required_cols = ['stock', 'foreignerBuyVolume', 'foreignerSellVolume', 'time']
                missing_cols = [col for col in required_cols if col not in df.columns]
                if missing_cols:
                    logger.log('ERROR', f"Missing required columns: {missing_cols}")
                    return pd.DataFrame()
                
                # Sort and calculate volume differences
                df.sort_values(['stock', 'timestamp', 'foreignerBuyVolume', 'foreignerSellVolume'], inplace=True)
                df['fBuyVol'] = df.groupby('stock')['foreignerBuyVolume'].diff()
                logger.log('INFO', f"fBuyVol has {df['fBuyVol'].isna().sum()} NaN values before fillna")

                df['fSellVol'] = df.groupby('stock')['foreignerSellVolume'].diff()
                logger.log('INFO', f"fSellVol has {df['fSellVol'].isna().sum()} NaN values before fillna")

                
                if put_through_data is not None and len(put_through_data) > 0:
                    # Convert put-through time from HH:MM:SS to seconds
                    def time_to_seconds(time_str):
                        """Convert time string to seconds from midnight."""
                        h, m, s = map(int, time_str.split(':'))
                        return h * 3600 + m * 60 + s
                        
                    # Process put-through data
                    df_tt = (put_through_data[put_through_data['stockSymbol'].isin(Globs.STOCKS)]
                            [['stockSymbol', 'vol', 'createdAt']]
                            .rename(columns={'stockSymbol': 'stock', 'createdAt': 'time_str'}))
                    
                    # Convert time to seconds and group
                    df_tt['time'] = df_tt['time_str'].apply(time_to_seconds)
                    df_tt = (df_tt.groupby(['stock', 'time'])
                            .agg({'vol': 'sum'})
                            .reset_index()
                            .query('time <= 53159'))  # 14:45:59 in seconds
                    
                    # Log aggregated put-through data
                    total_pt_volume = df_tt['vol'].sum()
                    unique_stocks_pt = df_tt['stock'].nunique()
                    logger.log('INFO', f"Total put-through volume: {total_pt_volume:,.0f} across {unique_stocks_pt} stocks")

                                    
                    # Merge with main data using merge_asof
                    before_merge_rows = len(df)
                    df = pd.merge_asof(
                        df.sort_values('time'),
                        df_tt[['stock', 'time', 'vol']].sort_values('time'),
                        by='stock',
                        on='time',
                        tolerance=15,  # 15 seconds tolerance
                        direction='nearest'
                    )
                    after_merge_rows = len(df)

                    # Log merge results
                    logger.log('INFO', f"After merge_asof: {after_merge_rows} rows (from {before_merge_rows})")
                    non_null_vol = df['vol'].notnull().sum()
                    logger.log('INFO',
                            f"Rows with matched put-through volume: {non_null_vol} ({non_null_vol / after_merge_rows * 100:.2f}%)")

                else:
                    logger.log('INFO', "No put-through data available")
                    df['vol'] = np.nan
                
                # Calculate monetary values
                df['fBuyVal'] = df['fBuyVol'] * df['close']
                df['fSellVal'] = df['fSellVol'] * df['close']
                
                # Identify and filter put-through transactions
                pt_mask = ((df['fBuyVol'].between(df['vol'] * (1 - tolerance), df['vol'] * (1 + tolerance))) |
                          (df['fSellVol'].between(df['vol'] * (1 - tolerance), df['vol'] * (1 + tolerance))))
                
                # Log put-through mask results
                pt_count = pt_mask.sum()
                logger.log('INFO', f"Identified {pt_count} rows as put-through transactions ({pt_count / len(df) * 100:.2f}% of data)")
                                
                return df[~pt_mask][['stock', 'time', 'fBuyVal','fSellVal']]
            
            except Exception as e:
                logger.log('ERROR', f"Error in calculate_foreign_fubon: {str(e)}")
                return pd.DataFrame()
        
        @staticmethod
        def calculate_bidask(df: pd.DataFrame):
            """
            Calculate bid and ask values from order book data.
            
            Computes the weighted sum of the top 3 bid and ask levels,
            normalized to billions.
            
            Args:
                df (pd.DataFrame): DataFrame with bid/ask price and volume columns
                
            Returns:
                None: Updates the DataFrame in-place, adding 'bid' and 'ask' columns
            """
            # Calculate Bid/Ask (in billions)
            df['bid'] = (
                df['bestBid1'] * df['bestBid1Volume'] +
                df['bestBid2'] * df['bestBid2Volume'] +
                df['bestBid3'] * df['bestBid3Volume']
            ) / 1e9

            df['ask'] = (
                df['bestOffer1'] * df['bestOffer1Volume'] +
                df['bestOffer2'] * df['bestOffer2Volume'] +
                df['bestOffer3'] * df['bestOffer3Volume']
            ) / 1e9

    @classmethod
    def load_history_data(cls, from_day=None) -> pd.DataFrame:
        """
        Load historical stock data from the plasma store.
        
        Args:
            from_day (str, optional): Starting date in format 'YYYY_MM_DD'
            
        Returns:
            pd.DataFrame: Historical stock data from plasma
        """
        # Load data from plasma store
        df: pd.DataFrame = Adapters.load_stock_data_from_plasma()
        
        # Drop unnecessary columns
        df = df.drop(['accNetBusd', 'accNetBusd2', 'return'], axis=1, level=0)

        # Filter by date if specified
        if from_day is not None:
            from_stamp = Utils.day_to_timestamp(from_day)
            df = df[df.index >= from_stamp]
        
        # Convert index to seconds
        df.index = df.index // 1e9

        logger.log("INFO", f"Loaded {len(df)} history records from plasma starting from {from_day}")

        return df

    @classmethod
    def load_realtime_data(cls, r, day, start, end) -> pd.DataFrame:
        """
        Load realtime stock data from Redis for the specified range.
        
        Args:
            r: Redis client connection
            day (str): Date in format 'YYYY_MM_DD'
            start (int): Starting position in Redis list
            end (int): Ending position in Redis list (-1 for until end)
            
        Returns:
            pd.DataFrame: Raw realtime stock data with all required stocks
        """
        current_timestamp = int(time.time())
        df = Adapters.RedisAdapters.load_realtime_stock_data_from_redis(r, day, start, end)

        # Validate required columns
        required_cols = list(cls.REALTIME_HOSE500_COLUMNS_MAPPING.keys())
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.log('ERROR', f"Missing columns in input data: {missing_cols}")
            return pd.DataFrame()

        if df.empty:
            logger.log('WARNING', "Empty realtime dataframe in load_realtime_data")
            return pd.DataFrame()

        try:
            # Rename columns to standardized names
            df = df.rename(columns=cls.REALTIME_HOSE500_COLUMNS_MAPPING)

            if 'stock' in df.columns and 'timestamp' in df.columns:
                # Filter data with timestamp < current_timestamp
                filtered_df = df[df['timestamp'] < current_timestamp]

                # Get counts per stock
                stock_counts = filtered_df['stock'].value_counts()

                # Log counts for each stock in Globs.STOCKS that has data
                log_data = {}
                for stock in Globs.STOCKS:
                    if stock in stock_counts and stock_counts[stock] > 0:
                        log_data[stock] = stock_counts[stock]

                if log_data:
                    # Sort by number of data points (descending)
                    sorted_data = {k: v for k, v in sorted(log_data.items(), key=lambda item: item[1], reverse=True)}
                    logger.log('INFO', f"Stock data points with timestamp < current: {sorted_data}")

            # Ensure all required stocks are present
            df = cls.ensure_all_stocks_present(df, Globs.STOCKS, data_name="New data")
            return df
        except Exception as e:
            logger.log('ERROR', f"Error in load_realtime_stock_data: {str(e)}")
            return pd.DataFrame()
        
    @classmethod
    def preprocess_realtime_data(cls, df: pd.DataFrame, day) -> pd.DataFrame:
        """
        Preprocess loaded realtime stock data before resampling.
        
        This method:
        1. Filters and transforms raw stock data
        2. Calculates derived metrics (OHLC, matchingValue, buy/sell indicators)
        3. Computes bid/ask metrics
        4. Calculates foreign transaction values
        
        Args:
            df (pd.DataFrame): Raw data loaded from Redis
            day (str): Date in format 'YYYY_MM_DD'
            
        Returns:
            pd.DataFrame: Preprocessed data ready for resampling
        """
        try:
            if df.empty:
                logger.log('WARNING', "Empty input dataframe in preprocessing")
                return pd.DataFrame()
                
            df = df.copy()
            
            # Filter for valid stocks and prices
            df = df[df['stock'].isin(Globs.STOCKS) & (df['close'] != 0)].copy()

            # Adjust timestamp (convert to seconds and add GMT+7 offset)
            df['timestamp'] = df['timestamp'] // 1000 + 7*60*60
            df['time'] = df['timestamp'] % 86400

            # Sort data for accurate calculations
            df = df.sort_values(['stock', 'timestamp', 'totalMatchVolume'])

            # Define function to calculate difference in series
            def diff(df: pd.DataFrame, column_name):
                """Calculate difference between consecutive values."""
                df = df.copy()
                df = df.sort_values(['timestamp', column_name])
                df['matchingValue'] = df.groupby('stock')[column_name].diff()
                return df

            # Calculate matching value from available metrics
            if 'matchingValue' not in df.columns:
                if 'matchingVolume' in df.columns:
                    df['matchingValue'] = df['matchingVolume'] * df['last']
                elif 'totalMatchValue' in df.columns:
                    df = diff(df, 'totalMatchValue')
                elif 'totalMatchVolume' in df.columns:
                    df = diff(df, 'totalMatchVolume')
                    df['matchingValue'] = df['matchingValue']
                else:
                    raise ValueError("Cannot calculate `matchingValue`")
            
            # Convert matching value to billions and ensure non-negative
            df['matchingValue'] = np.where(df['matchingValue'] < 0, 0, df['matchingValue']) / 1e9

            # Set OHLC values (initially all equal to close price)
            df['open'] = df['high'] = df['low'] = df['close'].copy()

            # Determine buy/sell classification based on price movement
            df['price_diff'] = df.groupby('stock')['close'].diff()
            bu_cond = (df['price_diff'] > 0)
            sd_cond = (df['price_diff'] < 0)
            df['matchedBy2'] = np.where(bu_cond, 1, np.where(sd_cond, -1, np.NaN))
            df['matchedBy2'] = df.groupby('stock')['matchedBy2'].ffill()
            df['matchedBy2'] = df['matchedBy2'].fillna(0)

            # Use existing matchedBy if available, otherwise use calculated value
            if 'matchedBy' not in df.columns:   
                df['matchedBy'] = df['matchedBy2']

            # Compute buy/sell values based on matched direction
            # Original matchedBy indicator
            df['bu'] = np.where(df['matchedBy'] == 1, df['matchingValue'], 0)
            df['sd'] = np.where(df['matchedBy'] == -1, df['matchingValue'], 0)

            # Recalculated matchedBy2 indicator
            df['bu2'] = np.where(df['matchedBy2'] == 1, df['matchingValue'], 0)
            df['sd2'] = np.where(df['matchedBy2'] == -1, df['matchingValue'], 0)

            # Clean up temporary columns
            df.drop(['matchedBy2', 'matchedBy'], axis=1, inplace=True)

            # Calculate bid/ask values
            cls.ComputeStats.calculate_bidask(df)

            # Define aggregation dictionary for second-level grouping
            agg_dic = {
                'timestamp': 'last', 
                'open': 'first', 
                'high': 'max', 
                'low': 'min', 
                'close': 'last', 
                'foreignerBuyVolume': 'last',
                'foreignerSellVolume': 'last',
                'matchingValue': 'sum',
                'bu': 'sum', 
                'sd': 'sum',
                'bu2': 'sum',
                'sd2': 'sum', 
                'bid': 'last', 
                'ask': 'last',
                'refPrice': 'last'
            }

            # Group data by stock and time (seconds) first
            df = df.groupby(['stock', 'time'], as_index=False).agg(agg_dic).reset_index()

            # Calculate foreign transaction values
            dftt = Adapters.load_put_through_data_realtime(rlv2, day)
            df_foreign = cls.ComputeStats.calculate_foreign_fubon(df, dftt)
            df = df.merge(df_foreign, how='left', on=['stock', 'time'])
            df['fBuyVal'] = df['fBuyVal'].fillna(0)
            df['fSellVal'] = df['fSellVal'].fillna(0)

            return df[cls.REQUIRED_COLUMNS]
        
        except Exception as e:
            logger.log('ERROR', f"Error in preprocess_realtime_stock_data: {str(e)}")
            return pd.DataFrame()
        

    @classmethod
    def postprocess_resampled_data(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Postprocess data after resampling to ensure data integrity.
        
        This method fills missing values, carries forward reference prices,
        and ensures OHLC values are consistent.
        
        Args:
            df (pd.DataFrame): Resampled data
            
        Returns:
            pd.DataFrame: Processed data with missing values filled appropriately
        """
        # Fill missing reference price values
        df['refPrice'] = df['refPrice'].ffill().bfill()
        df['close'] = df['close'].ffill()
        
        # Fill bid/ask values
        df['bid'] = df['bid'].ffill().fillna(0)
        df['ask'] = df['ask'].ffill().fillna(0)
        
        # Fill numerical metrics with zeros
        df['matchingValue'] = df['matchingValue'].fillna(0)
        df['bu'] = df['bu'].fillna(0)
        df['sd'] = df['sd'].fillna(0)
        df['bu2'] = df['bu2'].fillna(0)
        df['sd2'] = df['sd2'].fillna(0)
        df['fBuyVal'] = df['fBuyVal'].fillna(0)
        df['fSellVal'] = df['fSellVal'].fillna(0)

        # Fill OHLC values consistently
        df['close'] = df['close'].fillna(df['refPrice'])
        df['open'] = df['open'].fillna(df['close'])
        df['high'] = df['high'].fillna(df['close'])
        df['low'] = df['low'].fillna(df['close'])

        return df

    @classmethod
    def get_last_states(cls, df: pd.DataFrame, seconds=None, num_dps=None) -> pd.DataFrame:
        """
        Get last states of data for maintaining continuity between processing cycles.
        
        This method extracts recent data points to maintain state for differential calculations.
        Either `seconds` or `num_dps` should be provided, but not both.
        
        Args:
            df (pd.DataFrame): Dataset to extract last states from
            seconds (int, optional): Number of seconds of data to include
            num_dps (int, optional): Number of data points per stock to include
            
        Returns:
            pd.DataFrame: Subset of data representing the last states
            
        Raises:
            ValueError: If the input DataFrame is empty or lacks required columns
        """
        try:
            if df.empty:
                logger.log('WARNING', "Empty DataFrame in get_last_states")
                return pd.DataFrame()
                
            # Verify required columns
            required_columns = ['stock', 'timestamp']
            for col in required_columns:
                if col not in df.columns:
                    raise ValueError(f"Required column '{col}' missing from DataFrame")
            
            # Filter by time window
            if seconds is not None:
                timestamp = df['timestamp'].max()
                from_timestamp = timestamp - seconds*1000
                
                # Filter data within the time window
                last_states = df[(df['stock'].isin(Globs.STOCKS)) & 
                                (df['timestamp'] >= from_timestamp)].copy()
                
                # Check for missing stocks
                present_stocks = set(last_states['stock'].unique())
                missing_stocks = [s for s in Globs.STOCKS if s not in present_stocks]
                
                # Add most recent data for missing stocks
                if missing_stocks:
                    logger.log('WARNING', f"Missing stocks in last states: {len(missing_stocks)} stocks")
                    # Get latest records for missing stocks from original data
                    missing_records = []
                    for stock in missing_stocks:
                        stock_data = df[df['stock'] == stock]
                        if not stock_data.empty:
                            # Get most recent record
                            latest_record = stock_data.loc[stock_data['timestamp'].idxmax()]
                            missing_records.append(latest_record)
                    
                    # Add missing records if any found
                    if missing_records:
                        missing_df = pd.DataFrame(missing_records)
                        last_states = pd.concat([last_states, missing_df])
                
                return last_states
            
            # Filter by number of data points per stock
            elif num_dps is not None:
                if num_dps <= 0:
                    raise ValueError(f"Number of data points must be positive, got {num_dps}")
                    
                # Take last N data points per stock
                return df.groupby('stock').tail(num_dps)
            
            else:
                # If neither parameter is provided, return empty DataFrame
                logger.log('WARNING', "Neither seconds nor num_dps provided to get_last_states")
                return pd.DataFrame()
                
        except Exception as e:
            error_msg = f"Error in get_last_states: {str(e)}"
            logger.log('ERROR', error_msg)
            # Return empty DataFrame but log the error
            return pd.DataFrame()

    @classmethod
    def process_signals(cls):
        """
        Process market signals based on the resampled data.
        
        This method uses Celery to parallelize signal computation for
        all configured strategies and stores results in the plasma store.
        """
        from danglib.pslab.pslab_worker import compute_signals, clean_redis
        from tqdm import tqdm 
        import time

        class CeleryTaskError(Exception):
            """Exception raised when a Celery task fails."""
            pass

        task_ls = []
        active_tasks = set()
        conditions = cls.STRATEGIES

        # Submit tasks with progress tracking
        with tqdm(total=len(conditions), desc="Submitting and processing tasks") as pbar:
            conditions_iter = iter(conditions)
            submitted_count = 0

            while submitted_count < len(conditions):
                # Update active tasks set
                active_tasks = {task for task in active_tasks 
                              if task.status not in ['SUCCESS', 'FAILURE']}
                
                # Submit new tasks up to concurrency limit
                while len(active_tasks) < 5 and submitted_count < len(conditions):
                    try:
                        condition = next(conditions_iter)
                        task = compute_signals.delay(strategy=condition)
                        task_ls.append(task)
                        active_tasks.add(task)
                        submitted_count += 1
                        pbar.update(1)
                    except StopIteration:
                        break
                
                time.sleep(0.1)

        # Monitor task completion
        with tqdm(total=len(task_ls), desc="Processing tasks") as pbar:
            completed_tasks = set()
            while len(completed_tasks) < len(task_ls):
                for i, task in enumerate(task_ls):
                    if i not in completed_tasks and task.status in ['SUCCESS', 'FAILURE']:
                        if task.status == 'FAILURE':
                            raise CeleryTaskError(f"Task failed: {task.id}")
                        completed_tasks.add(i)
                        pbar.update(1)
                time.sleep(0.1)

        # Collect results
        chunk_results = []
        with tqdm(total=len(task_ls), desc="Collecting results") as pbar:
            for task in task_ls:
                try:
                    result = task.result
                    if result is not None:
                        chunk_results.append(result)
                except Exception as e:
                    print(f"Error collecting results for strategy {task}: {str(e)}")
                pbar.update(1)

        # Combine results and save to plasma
        if chunk_results:
            df = pd.concat(chunk_results)
            
            # Save to plasma store
            _, disconnect, psave, pload = gen_plasma_functions(db=Globs.PLASMA_DB)
            psave('pslab_strategies_realtime_signals', df)
        
        # Clean up Redis after processing
        clean_redis()