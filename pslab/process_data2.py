import pandas as pd
import numpy as np
from redis import StrictRedis
from datetime import datetime
import time
from abc import ABC, abstractmethod

from danglib.utils import check_run_with_interactive
from danglib.pslab.utils import Utils
from danglib.pslab.resources import Adapters, Globs
from danglib.lazy_core import gen_plasma_functions
from danglib.pslab.logger_module import DataLogger
logger = DataLogger("/home/ubuntu/Dang/project_ps/logs", file_prefix="processdata")

# Redis clients
r = StrictRedis(decode_responses=True)
rlv2 = StrictRedis(host='lv2', decode_responses=True)


class ProcessData(ABC):
    """Abstract base class for data processing"""
    
    SUBGROUP = None
    
    @classmethod
    @abstractmethod
    def load_history_data(cls, from_day=None) -> pd.DataFrame:
        """Load historical data from data source"""
        pass
    
    @classmethod
    @abstractmethod
    def load_realtime_data(cls, redis_client, day, start, end) -> pd.DataFrame:
        """Load realtime data from Redis"""
        pass

    @classmethod
    @abstractmethod
    def preprocess_realtime_data(cls, df: pd.DataFrame, day) -> pd.DataFrame:
        """Preprocess loaded realtime data"""
        pass

    @classmethod
    @abstractmethod
    def postprocess_resampled_data(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Postprocess data after resampling"""
        pass

    @classmethod
    @abstractmethod
    def get_agg_dic(cls) -> dict:
        """Get aggregation dictionary for resampling"""
        pass

    @classmethod
    @abstractmethod
    def get_last_states(cls, df: pd.DataFrame, seconds=None, num_dps=None) -> pd.DataFrame:
        """Get last states of data"""
        pass


class ProcessStockData(ProcessData):
    """Process stock market data"""
    
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

    @staticmethod
    def ensure_all_stocks_present(df: pd.DataFrame, required_stocks: list) -> pd.DataFrame:
        """
        Ensure all stocks are present in DataFrame. Add rows with NaN for missing stocks.
        
        Args:
            df: Original DataFrame
            required_stocks: List of required stock symbols
            
        Returns:
            DataFrame with all required stocks
        """
        if df.empty:
            logger.warning("Input DataFrame is empty in ensure_all_stocks_present")
            return df
            
        # Get max timestamp
        max_timestamp = df['timestamp'].max()
        max_time = df['time'].max()
        
        # Check existing stocks
        existing_stocks = set(df['stock'].unique())
        required_stocks_set = set(required_stocks)
        
        # Find missing stocks
        missing_stocks = required_stocks_set - existing_stocks
        
        if missing_stocks:
            logger.warning(f"Missing stocks in data: {missing_stocks}")
            
            # Create template for new rows
            template_row = pd.Series(index=df.columns, data=np.nan)
            
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
                logger.info(f"Added {len(new_rows)} rows for missing stocks")
        
        return df

    class ComputeStats:
        """Static methods for computing statistics on stock data"""
        
        @staticmethod
        def calculate_foreign_fubon(
            dfi: pd.DataFrame,
            put_through_data: pd.DataFrame = None,
            tolerance: float = 0.002
        ) -> pd.DataFrame:
            """
            Calculate foreign transactions for Fubon stocks
            
            Args:
                dfi: Preprocessed dataframe with stock data
                put_through_data: Put-through transaction data
                tolerance: Tolerance for matching put-through volumes
                
            Returns:
                DataFrame with foreign transactions
            """
            try:
                df = dfi.copy()
                logger.info(f"Processing foreign transactions for {len(df)} records")
    
                if df.empty:
                    logger.warning("Empty input dataframe for foreign calculations")
                    return pd.DataFrame()
                
                # Validate required columns
                required_cols = ['stock', 'foreignerBuyVolume', 'foreignerSellVolume', 'time']
                missing_cols = [col for col in required_cols if col not in df.columns]
                if missing_cols:
                    logger.error(f"Missing required columns: {missing_cols}")
                    return pd.DataFrame()
                
                # Sort and calculate volume differences
                df.sort_values(['stock', 'timestamp', 'foreignerBuyVolume', 'foreignerSellVolume'], inplace=True)
                df['fBuyVol'] = df.groupby('stock')['foreignerBuyVolume'].diff().fillna(df['foreignerBuyVolume'])
                df['fSellVol'] = df.groupby('stock')['foreignerSellVolume'].diff().fillna(df['foreignerSellVolume'])
                
                if put_through_data is not None and len(put_through_data) > 0:
                    # Convert put-through time from HH:MM:SS to seconds
                    def time_to_seconds(time_str):
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
                    
                    # Merge with main data using merge_asof
                    df = pd.merge_asof(
                        df.sort_values('time'),
                        df_tt[['stock', 'time', 'vol']].sort_values('time'),
                        by='stock',
                        on='time',
                        tolerance=15,
                        direction='nearest'
                    )
                else:
                    df['vol'] = np.nan
                
                df['fBuyVal'] = df['fBuyVol'] * df['close']
                df['fSellVal'] = df['fSellVol'] * df['close']
                
                # Identify and filter put-through transactions
                pt_mask = ((df['fBuyVol'].between(df['vol'] * (1 - tolerance), df['vol'] * (1 + tolerance))) |
                          (df['fSellVol'].between(df['vol'] * (1 - tolerance), df['vol'] * (1 + tolerance))))
                
                return df[~pt_mask][['stock', 'time', 'fBuyVal','fSellVal']]
            
            except Exception as e:
                logger.error(f"Error in calculate_foreign_fubon: {str(e)}", exc_info=True)
                return pd.DataFrame()
            
        @staticmethod
        def calculate_bidask(df: pd.DataFrame):
            """Calculate bid/ask values"""
            # Calculate Bid
            df['bid'] = (
                df['bestBid1'] * df['bestBid1Volume'] +
                df['bestBid2'] * df['bestBid2Volume'] +
                df['bestBid3'] * df['bestBid3Volume']
            ) / 1e9

            # Calculate Ask
            df['ask'] = (
                df['bestOffer1'] * df['bestOffer1Volume'] +
                df['bestOffer2'] * df['bestOffer2Volume'] +
                df['bestOffer3'] * df['bestOffer3Volume']
            ) / 1e9

    @classmethod
    def load_history_data(cls, from_day=None) -> pd.DataFrame:
        """Load historical stock data"""
        df = Adapters.load_stock_data_from_plasma()
        
        df = df.drop(['accNetBusd', 'accNetBusd2', 'return'], axis=1, level=0)

        if from_day is not None:
            from_stamp = Utils.day_to_timestamp(from_day)
            df = df[df.index >= from_stamp]
        df.index = df.index // 1e9

        return df

    @classmethod
    def load_realtime_data(cls, redis_client, day, start, end) -> pd.DataFrame:
        """Load realtime stock data from Redis"""
        logger.info(f"Loading realtime stock data for {day} from position {start} to {end}")
        
        df = Adapters.RedisAdapters.load_realtime_stock_data_from_redis(redis_client, day, start, end)

        # Validate required columns
        required_cols = list(cls.REALTIME_HOSE500_COLUMNS_MAPPING.keys())
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"Missing columns in input data: {missing_cols}")
            return pd.DataFrame()

        if df.empty:
            logger.warning("Empty realtime dataframe in load_realtime_data")
            return pd.DataFrame()

        try:
            df = df.rename(columns=cls.REALTIME_HOSE500_COLUMNS_MAPPING)
            df = cls.ensure_all_stocks_present(df, Globs.STOCKS)
            logger.debug(f"Loaded {len(df)} rows of realtime stock data")
            return df
        except Exception as e:
            logger.error(f"Error in load_realtime_stock_data: {str(e)}", exc_info=True)
            return pd.DataFrame()
        
    @classmethod
    def preprocess_realtime_data(cls, df: pd.DataFrame, day) -> pd.DataFrame:
        """Preprocess realtime stock data"""
        try:
            if df.empty:
                logger.warning("Empty input dataframe in preprocessing")
                return pd.DataFrame()
                
            logger.info(f"Preprocessing {len(df)} rows of realtime stock data")
            df = df.copy()
            df = df[df['stock'].isin(Globs.STOCKS) & (df['close'] != 0)].copy()

            # Adjust timestamp
            df['timestamp'] = df['timestamp'] // 1000 + 7*60*60
            df['time'] = df['timestamp'] % 86400

            df = df.sort_values(['stock', 'timestamp', 'totalMatchVolume'])

            # Calculate matchingValue
            def diff(df: pd.DataFrame, column_name):
                df = df.copy()
                df = df.sort_values(['timestamp', column_name])
                df['matchingValue'] = df.groupby('stock')[column_name].diff()
                return df

            if 'matchingValue' not in df.columns:
                if 'matchingVolume' in df.columns:
                    df['matchingValue'] = df['matchingVolume'] * df['last']
                elif 'totalMatchValue' in df.columns:
                    df = diff(df, 'totalMatchValue')
                elif 'totalMatchVolume' in df.columns:
                    df = diff(df, 'totalMatchVolume')
                    df['matchingValue'] = df['matchingValue']
                else:
                    raise ValueError("Can not calculate `matchingValue`")
            
            df['matchingValue'] = np.where(df['matchingValue'] < 0, 0, df['matchingValue']) / 1e9

            # Setup OHLC values
            df['open'] = df['high'] = df['low'] = df['close'].copy()

            # Calculate price differences for buy/sell classification
            df['price_diff'] = df.groupby('stock')['close'].diff()
            bu_cond = (df['price_diff'] > 0)
            sd_cond = (df['price_diff'] < 0)
            df['matchedBy2'] = np.where(bu_cond, 1, np.where(sd_cond, -1, np.NaN))
            df['matchedBy2'] = df.groupby('stock')['matchedBy2'].ffill()
            df['matchedBy2'] = df['matchedBy2'].fillna(0)

            if 'matchedBy' not in df.columns:   
                df['matchedBy'] = df['matchedBy2']

            # Compute buy/sell values
            df['bu'] = np.where(df['matchedBy'] == 1, df['matchingValue'], 0)
            df['sd'] = np.where(df['matchedBy'] == -1, df['matchingValue'], 0)
            df['bu2'] = np.where(df['matchedBy2'] == 1, df['matchingValue'], 0)
            df['sd2'] = np.where(df['matchedBy2'] == -1, df['matchingValue'], 0)

            df.drop(['matchedBy2', 'matchedBy'], axis=1, inplace=True)

            # Calculate bid/ask
            cls.ComputeStats.calculate_bidask(df)

            # Define aggregation dictionary
            agg_dic = {
                'timestamp': 'last', 
                'open': 'first', 
                'high': 'max', 
                'low': 'min', 
                'close': 'last', 
                'foreignerBuyVolume': 'sum',
                'foreignerSellVolume': 'sum',
                'matchingValue': 'sum',
                'bu': 'sum', 
                'sd': 'sum',
                'bu2': 'sum',
                'sd2': 'sum', 
                'bid': 'last', 
                'ask': 'last',
                'refPrice': 'last'
            }

            # Group data by stock and time (seconds)
            df = df.groupby(['stock', 'time'], as_index=False).agg(agg_dic).reset_index()

            # Calculate foreign data
            dftt = Adapters.load_put_through_data_realtime(rlv2, day)
            df_foreign = cls.ComputeStats.calculate_foreign_fubon(df, dftt)
            df = df.merge(df_foreign, how='left', on=['stock', 'time'])
            df['fBuyVal'] = df['fBuyVal'].fillna(0)
            df['fSellVal'] = df['fSellVal'].fillna(0)

            logger.debug(f"Preprocessing complete, output shape: {df.shape}")
            return df[cls.REQUIRED_COLUMNS]
        
        except Exception as e:
            logger.error(f"Error in preprocess_realtime_stock_data: {str(e)}", exc_info=True)
            return pd.DataFrame()

    @classmethod
    def postprocess_resampled_data(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Post-process resampled data"""
        logger.debug("Post-processing resampled data")
        
        # Fill missing values
        df['refPrice'] = df['refPrice'].ffill().bfill()
        df['close'] = df['close'].ffill()
        df['bid'] = df['bid'].ffill().fillna(0)
        df['ask'] = df['ask'].ffill().fillna(0)
        
        # Fill numerical columns with zeros
        for col in ['matchingValue', 'bu', 'sd', 'bu2', 'sd2', 'fBuyVal', 'fSellVal']:
            df[col] = df[col].fillna(0)

        # Fill OHLC values
        df['close'] = df['close'].fillna(df['refPrice'])
        df['open'] = df['open'].fillna(df['close'])
        df['high'] = df['high'].fillna(df['close'])
        df['low'] = df['low'].fillna(df['close'])

        return df

    @classmethod
    def get_last_states(cls, df: pd.DataFrame, seconds=None, num_dps=None) -> pd.DataFrame:
        """Get last states for each stock"""
        if seconds is not None:
            timestamp = df['timestamp'].max()
            fromstamp = timestamp - seconds*1000
            laststates = df[(df['stock'].isin(Globs.STOCKS)) & (df['timestamp'] >= fromstamp)].copy()

            missing_stocks = [s for s in Globs.STOCKS if s not in laststates['stock'].unique()]
            if missing_stocks:
                logger.warning(f"Missing stocks in last states: {missing_stocks}")
                df_miss = df[df['stock'].isin(missing_stocks)].copy()
                laststates = pd.concat([laststates, df_miss.groupby('stock').tail(1)])

        if num_dps is not None:
            laststates = df.groupby('stock').tail(num_dps)
            
        return laststates

    @classmethod
    def get_agg_dic(cls) -> dict:
        """Get aggregation dictionary for resampling"""
        return cls.RESAMPLE_AGG_DIC


class ProcessPsData(ProcessData):
    """Process PS (Futures) market data"""
    
    NAME = 'PS'
    SUBGROUP = None
    
    REQUIRED_RAW_COLLS = ['id', 'time', 'lastPrice', 'lastVol', 'buyForeignQtty', 'sellForeignQtty', 
                           'totalMatchValue', 'totalMatchVol']
    REQUIRED_OUT_COLLS = [
        'canldleTime', 'F1Open', 'F1High', 'F1Low', 'F1Close', 'F1Volume', 'F1Value', 
        'fF1BuyVol', 'fF1SellVol', 'fPsBuyVol', 'fPsSellVol', 'outstandingFPos'
    ]

    REALTIME_F1_COLUMNS_MAPPING = {
        'lastPrice': 'last',
        'totalMatchValue': 'totalValue',
        'totalMatchVol': 'totalVolume',
        'buyForeignQtty': 'foreignerBuyVolume',
        'sellForeignQtty': 'foreignerSellVolume',
    }

    @classmethod
    def load_history_data(cls, from_stamp) -> pd.DataFrame:
        """Load historical PS data"""
        df = Adapters.load_market_stats_from_plasma()
        df.index = df.index // 1e9

        if from_stamp is not None:
            df = df[df.index >= from_stamp]
        
        return df[['F1Value', 'F1Volume', 'outstandingFPos', 'fF1BuyVol', 'fF1SellVol', 
                   'fPsBuyVol', 'fPsSellVol', 'F1Open', 'F1High', 'F1Low', 'F1Close']]
    
    @classmethod
    def load_realtime_data(cls, redis_client, day, start, end) -> pd.DataFrame:
        """Load realtime PS data from Redis"""
        logger.info(f"Loading realtime {cls.NAME} data for {day} from position {start} to {end}")
        
        df = Adapters.RedisAdapters.load_realtime_PS_data_from_redis(redis_client, day, start, end)
        df = df[cls.REQUIRED_RAW_COLLS].copy()
        
        missing_cols = [col for col in cls.REQUIRED_RAW_COLLS if col not in df.columns]
        if missing_cols:
            logger.error(f"Missing columns in input {cls.NAME} data: {missing_cols}")
            return pd.DataFrame()

        if df.empty:
            logger.warning(f"Empty realtime dataframe in {cls.NAME} load_realtime_data")
            return pd.DataFrame()

        try:
            df = df.rename(columns=cls.REALTIME_F1_COLUMNS_MAPPING)
            df['day'] = day
            df['time'] = df['time'].astype(str).str[-6:]
            df['timestamp'] = pd.to_datetime(day + ' ' + df['time'], format='%Y_%m_%d %H%M%S').astype(int) // 1e9
            df['time'] = df['timestamp'] % 86400
            
            logger.debug(f"Loaded {len(df)} rows of realtime {cls.NAME} data")
            return df
        except Exception as e:
            logger.error(f"Error in load_realtime_{cls.NAME}_data: {str(e)}", exc_info=True)
            return pd.DataFrame()
    
    @classmethod
    def preprocess_realtime_data(cls, df: pd.DataFrame, day) -> pd.DataFrame:
        """Preprocess realtime PS data"""
        try:
            if df.empty:
                logger.warning(f"Empty input dataframe in {cls.NAME} preprocessing")
                return pd.DataFrame()
                
            logger.info(f"Preprocessing {len(df)} rows of realtime {cls.NAME} data")
            df = df.copy()
            df.sort_values(['foreignerBuyVolume', 'foreignerSellVolume', 'time', 'day'], inplace=True)
            
            # Process F1 data
            dff1 = df[df['id'] == 'f1'].copy()
            dff1 = dff1.sort_values('totalVolume')

            dff1['F1Open'] = dff1['F1High'] = dff1['F1Low'] = dff1['F1Close'] = dff1['last'].copy()
            dff1['F1Volume'] = dff1['totalVolume'].diff()
            dff1['F1Value'] = dff1['totalValue'].diff()
        
            dff1['fF1BuyVol'] = dff1['foreignerBuyVolume'].diff().fillna(dff1['foreignerBuyVolume'])
            dff1['fF1SellVol'] = dff1['foreignerSellVolume'].diff().fillna(dff1['foreignerSellVolume'])

            dff1['totalBidVolume'] = 0
            dff1['totalAskVolume'] = 0
            dff1['fPsBuyVol'] = dff1['fPsSellVol'] = dff1['outstandingFPos'] = 0

            required_cols = ['time', 'timestamp', 'F1Open', 'F1High', 'F1Low', 'F1Close', 'F1Volume', 
                            'F1Value', 'fF1BuyVol', 'fF1SellVol', 'fPsBuyVol', 'fPsSellVol', 'outstandingFPos']
            
            logger.debug(f"Preprocessing complete, output shape: {dff1[required_cols].shape}")
            return dff1[required_cols]

        except Exception as e:
            logger.error(f"Error in {cls.NAME} preprocess_realtime_data: {str(e)}", exc_info=True)
            return pd.DataFrame()
        
    @classmethod
    def postprocess_resampled_data(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Post-process resampled data"""
        df[['fF1BuyVol', 'fF1SellVol']] = df[['fF1BuyVol', 'fF1SellVol']].fillna(0)
        return df
    
    @classmethod
    def get_last_states(cls, df: pd.DataFrame, seconds=None, num_dps=None) -> pd.DataFrame:
        """Get last states"""
        if seconds is not None:
            timestamp = df['timestamp'].max()
            fromstamp = timestamp - seconds
            laststates = df[(df['timestamp'] >= fromstamp)].copy()
        if num_dps is not None:
            laststates = df.groupby('id').tail(num_dps)
        return laststates

    @classmethod
    def get_agg_dic(cls, stats=None) -> dict:
        """Get aggregation dictionary for resampling"""
        if stats is None:
            stats = cls.REQUIRED_OUT_COLLS

        return {
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
            'F1Close': 'last'
        }


class Resampler:
    """Class for resampling time series data"""
    
    # Constants for trading time calculations
    TIME_CONSTANTS = {
        'MORNING': {
            'START': 9 * 3600 + 15 * 60,    # 9:15
            'END': 11 * 3600 + 30 * 60      # 11:30
        },
        'AFTERNOON': {
            'START': 13 * 3600,             # 13:00
            'END': 14 * 3600 + 30 * 60      # 14:30
        },
        'ATC': 14 * 3600 + 45 * 60         # 14:45
    }

    @staticmethod
    def _create_time_filter(df: pd.DataFrame, time_range: dict) -> pd.Series:
        """Create time filter for a specific trading session"""
        return (df['time'] >= time_range['START']) & (df['time'] < time_range.get('END', float('inf')))

    @classmethod
    def cleanup_timestamp(cls, day: str, df: pd.DataFrame) -> pd.DataFrame:
        """Clean up timestamp data for proper trading session handling"""
        logger.debug(f"Cleaning up timestamps for {len(df)} rows of data")
        
        df = df.copy()
        tc = cls.TIME_CONSTANTS
        
        # Create filters for different trading sessions
        filters = {
            'morning': cls._create_time_filter(df, tc['MORNING']),
            'afternoon': cls._create_time_filter(df, tc['AFTERNOON']),
            'atc': df['time'] >= tc['ATC']
        }
        
        # Apply filters
        df = df[filters['morning'] | filters['afternoon'] | filters['atc']].copy()
        
        # Set ATC time
        df.loc[filters['atc'], 'time'] = tc['ATC']
        atc_stamp = int(pd.to_datetime(day + ' 14:45:00', format='%Y_%m_%d %H:%M:%S').timestamp())
        df.loc[filters['atc'], 'timestamp'] = atc_stamp
        
        logger.debug(f"After timestamp cleanup: {len(df)} rows")
        return df

    @classmethod
    def _calculate_candletime(cls, timestamps: pd.Series, timeframe: str) -> pd.Series:
        """Calculate candle time based on timestamps and timeframe"""
        tc = cls.TIME_CONSTANTS
        
        # Calculate base timestamps
        day_start = timestamps // 86400 * 86400
        seconds_in_day = timestamps % 86400
        ts_915 = day_start + tc['MORNING']['START']
        ts_afternoon = day_start + tc['AFTERNOON']['START']
        
        def _resample(ts: pd.Series, tf: str) -> pd.Series:
            """Calculate base candle times"""
            ts_started = day_start + 9 * 3600
            tf_seconds = Globs.TF_TO_MIN.get(tf) * 60
            return (ts - ts_started) // tf_seconds * tf_seconds + ts_started
        
        # Calculate new candles
        new_candles = _resample(timestamps, timeframe)
        new_candles_time = new_candles % 86400
        
        # Apply time adjustments
        conditions = [
            (new_candles_time <= tc['MORNING']['START']),
            (new_candles_time.between(tc['MORNING']['END'], tc['AFTERNOON']['START']))
        ]
        choices = [ts_915, ts_afternoon]
        
        new_candles = pd.Series(
            np.select(conditions, choices, new_candles),
            index=timestamps.index
        )
        
        # Handle ATC for non-daily timeframes
        if timeframe != '1D':
            new_candles = pd.Series(
                np.where(
                    seconds_in_day == tc['ATC'],
                    timestamps,
                    new_candles
                ),
                index=timestamps.index
            )
        
        return new_candles
    
    @classmethod
    def transform(cls, day: str, df: pd.DataFrame, timeframe: str, agg_dic: dict, 
                  cleanup_data: bool = False, subgroup=None) -> pd.DataFrame:
        """
        Transform data by resampling to specified timeframe
        
        Args:
            day: Trading day
            df: DataFrame to transform
            timeframe: Target timeframe
            agg_dic: Aggregation dictionary
            cleanup_data: Whether to clean up timestamps
            subgroup: Column to use for subgrouping
            
        Returns:
            Resampled DataFrame
        """
        logger.info(f"Resampling data to {timeframe} timeframe")
        
        df = df.copy()
        if cleanup_data:
            df = cls.cleanup_timestamp(day, df)
        
        # Calculate new candle times
        df['candleTime'] = cls._calculate_candletime(df['timestamp'], timeframe)

        # Define grouping columns
        groups = ['candleTime']
        if subgroup:
            groups.append(subgroup)

        # Perform aggregation
        df = df.groupby(by=groups).agg(agg_dic).reset_index()
        if 'timestamp' in df.columns:
            df = df.drop('timestamp', axis=1)

        # Pivot if subgroup is specified
        if subgroup:
            df = df.pivot(index='candleTime', columns=subgroup)
        else:
            df = df.set_index('candleTime')

        logger.debug(f"After resampling: {df.shape} shape")
        return df


class Aggregator:
    """Class for aggregating and managing real-time data processing"""
    
    # Trading time constants
    TRADING_HOURS = {
        'MORNING_START': datetime.strptime('09:15', '%H:%M').time(),
        'LUNCH_START': datetime.strptime('11:30', '%H:%M').time(),
        'LUNCH_END': datetime.strptime('13:00', '%H:%M').time(),
        'AFTERNOON_END': datetime.strptime('14:50', '%H:%M').time()
    }
    
    # Sleep durations (in seconds)
    SLEEP_DURATION = {
        'OUTSIDE_TRADING': 60,
        'EMPTY_DATA': 1,
        'ERROR': 1
    }

    def __init__(self, day: str, timeframe: str, output_plasma_key: str, data_processor: ProcessData):
        """
        Initialize the Aggregator
        
        Args:
            day: Trading day
            timeframe: Target timeframe
            output_plasma_key: Key for storing results in plasma
            data_processor: Data processor class
        """
        self.day = day
        self.timeframe = timeframe
        self.timeframe_seconds = Globs.TF_TO_MIN.get(timeframe) * 60
        self.r = StrictRedis(decode_responses=True)
        self.output_key = f"{output_plasma_key}.{timeframe}"
        self.data_processor = data_processor

        self.current_pos = 0 
        self.current_candle_stamp = 0
        self.resampled_df = pd.DataFrame()
        self.last_states = pd.DataFrame()
        
        logger.info(f"Initialized Aggregator for {day} with timeframe {timeframe}")

    def join_historical_data(self, df: pd.DataFrame, ndays: int = 10) -> pd.DataFrame:
        """
        Join new data with historical data
        
        Args:
            df: New data
            ndays: Number of historical days to include
            
        Returns:
            Merged DataFrame
        """
        from_day = Adapters.get_historical_day_by_index(-ndays)
        logger.info(f'Merging with historical data from {from_day}...')

        # Load historical data
        df_his = self.data_processor.load_history_data(from_day)
        
        # Merge and sort data
        df_merge = pd.concat([df_his, df])
        df_merge = df_merge.sort_index(ascending=True)

        logger.info(f'Historical data merged. Combined shape: {df_merge.shape}')
        return df_merge
    
    def _get_next_candle_stamp(self, current_timestamp: int) -> int:
        """Calculate timestamp for the next candle"""
        current_candle = self._get_candle_stamp(current_timestamp)
        return current_candle + self.timeframe_seconds
    
    def _get_candle_stamp(self, timestamp: int) -> int:
        """Calculate timestamp for the current candle"""
        return (timestamp // self.timeframe_seconds) * self.timeframe_seconds

    def _add_empty_candle(self, candlestamp: int, df: pd.DataFrame) -> pd.DataFrame:
        """Add an empty candle with NaN values to the DataFrame"""
        # Create new DataFrame with empty row
        new_df = pd.DataFrame(
            index=[candlestamp],
            columns=df.columns,
            data=np.nan
        )
        
        # Concat with existing DataFrame
        return pd.concat([df, new_df])
    
    def _update_last_states(self, df: pd.DataFrame, seconds: int = 30) -> None:
        """Update last states from DataFrame"""
        logger.debug(f"Updating last states from {len(df)} rows")
        last_states = self.data_processor.get_last_states(df, seconds=seconds)
        self.last_states = last_states
        logger.debug(f"Updated last states with {len(last_states)} rows")

    def _append_last_states(self, df: pd.DataFrame) -> pd.DataFrame:
        """Append last states to DataFrame for accurate diff calculation"""
        if not self.last_states.empty:
            logger.debug(f"Appending {len(self.last_states)} last states to {len(df)} new rows")
            return pd.concat([self.last_states, df])
        return df
    
    @staticmethod
    def merge_resampled_data(old_df: pd.DataFrame, new_df: pd.DataFrame, agg_dict: dict) -> pd.DataFrame:
        """
        Merge and recalculate values for overlapping candles based on aggregation dictionary
        
        Args:
            old_df: Existing DataFrame
            new_df: New DataFrame
            agg_dict: Aggregation dictionary
            
        Returns:
            Merged DataFrame
        """
        if old_df.empty:
            return new_df
            
        # Find common indices (candleTimes)
        common_idx = old_df.index.intersection(new_df.index)
        
        # Ensure columns are properly named
        old_df.columns.names = ['stats', 'stock']
        new_df.columns.names = ['stats', 'stock']
        
        if not common_idx.empty:
            logger.info(f"Merging data with {len(common_idx)} overlapping candles")
            
            # Combine data for overlapping candles
            overlap_data = pd.concat([
                old_df.loc[common_idx],
                new_df.loc[common_idx]
            ])

            # Recalculate values using aggregation dictionary
            recalculated = overlap_data.stack()
            recalculated = recalculated.groupby(level=[0, 1]).agg(agg_dict).pivot_table(index='candleTime', columns='stock')

            # Combine all data
            result_df = pd.concat([
                old_df.loc[old_df.index.difference(common_idx)],  # Rows only in old
                recalculated,                                     # Recalculated rows
                new_df.loc[new_df.index.difference(common_idx)]   # Rows only in new
            ]).sort_index()
            
            logger.debug(f"After merge: {result_df.shape} shape")
        else:
            # If no overlap, simply concat
            result_df = pd.concat([old_df, new_df]).sort_index()
            logger.debug(f"No overlap, simple concat: {result_df.shape} shape")
        
        return result_df
    
    def _store_resampled_data(self, df: pd.DataFrame) -> None:
        """Store resampled data to plasma database"""
        logger.info(f"Storing resampled data to plasma key: {self.output_key}")
        _, disconnect, psave, pload = gen_plasma_functions(db=Globs.PLASMA_DB)
        psave(self.output_key, df)
        logger.debug("Data stored successfully")
    
    def start_realtime_resampling(self) -> None:
        """Start real-time data resampling process"""
        logger.info(f"Starting realtime resampling with timeframe: {self.timeframe}")

        while True:
            try:
                current_datetime = datetime.now()
                time_str = current_datetime.strftime('%H:%M:%S')
                current_time = current_datetime.time()

                # Skip if outside trading hours
                if not (self.TRADING_HOURS['MORNING_START'] <= current_time <= self.TRADING_HOURS['AFTERNOON_END']):
                    logger.info(f"Outside trading hours ({time_str}), sleeping")
                    time.sleep(self.SLEEP_DURATION['OUTSIDE_TRADING'])
                    continue

                # Skip during lunch break
                if self.TRADING_HOURS['LUNCH_START'] <= current_time <= self.TRADING_HOURS['LUNCH_END']:
                    logger.info(f"Lunch break ({time_str}), sleeping")
                    time.sleep(self.SLEEP_DURATION['OUTSIDE_TRADING'])
                    continue

                # Load new data from Redis
                logger.info(f"Processing data from position {self.current_pos}")
                raw_data = self.data_processor.load_realtime_data(self.r, self.day, self.current_pos, -1)
                self.current_pos += len(raw_data) + 1
                logger.debug(f"Updated current_pos to {self.current_pos}")

                if not raw_data.empty:
                    # Combine with last states for accurate calculations
                    raw_data_with_last_states = self._append_last_states(raw_data)

                    # Preprocess data
                    new_data = self.data_processor.preprocess_realtime_data(raw_data_with_last_states, self.day)
                    new_data = new_data[new_data['timestamp'] >= self.current_candle_stamp]
                    
                    if hasattr(new_data, 'stock'):
                        logger.info(f"New data contains {len(new_data['stock'].unique())} unique stocks")

                    # Update last states for future processing
                    self._update_last_states(raw_data_with_last_states, seconds=30)

                    if not new_data.empty:
                        # Resample preprocessed data
                        new_resampled = Resampler.transform(
                            day=self.day,
                            df=new_data,
                            timeframe=self.timeframe,
                            agg_dic=self.data_processor.get_agg_dic(),
                            cleanup_data=True,
                            subgroup=self.data_processor.SUBGROUP
                        )
                        logger.info(f"Resampled {len(new_data)} rows into {len(new_resampled)} candles")

                        # Initialize or update resampled DataFrame
                        if self.resampled_df.empty:
                            self.resampled_df = self.join_historical_data(new_resampled, ndays=1)
                            logger.info(f"Initialized resampled dataframe with shape: {self.resampled_df.shape}")
                        else:
                            # Merge with existing data
                            self.resampled_df = self.merge_resampled_data(
                                old_df=self.resampled_df, 
                                new_df=new_resampled,
                                agg_dict=self.data_processor.get_agg_dic()
                            )
                            logger.info(f"Updated resampled dataframe, new shape: {self.resampled_df.shape}")
                        
                        # Post-process the merged data
                        self.resampled_df = self.data_processor.postprocess_resampled_data(self.resampled_df)

                        # Update candle timestamp
                        current_candle_stamp = self._get_candle_stamp(new_data['timestamp'].max())
                        if current_candle_stamp > self.current_candle_stamp:
                            self.current_candle_stamp = current_candle_stamp
                            logger.info(f"Updated current_candle_stamp to {self.current_candle_stamp}")
                        else:
                            # Add empty candle for next timeframe
                            next_candle = self._get_next_candle_stamp(self.current_candle_stamp)
                            self.current_candle_stamp = next_candle
                            self.resampled_df = self._add_empty_candle(self.current_candle_stamp, self.resampled_df)
                            logger.info(f"Added empty candle at {self.current_candle_stamp}")

                        # Store processed data
                        self._store_resampled_data(self.resampled_df)
                else:
                    logger.warning("No new data received")
                    time.sleep(self.SLEEP_DURATION['EMPTY_DATA'])
                    continue

                # Calculate next candle time and sleep until then
                current_stamp = time.time() + 7*3600
                next_candle_time = self._get_next_candle_stamp(current_stamp)
                sleep_duration = next_candle_time - current_stamp

                if sleep_duration > 0:
                    logger.info(f"Sleeping for {sleep_duration:.2f} seconds until next candle")
                    time.sleep(sleep_duration)

                logger.info(f"Completed processing cycle, next candle at {datetime.fromtimestamp(next_candle_time).strftime('%H:%M:%S')}")

            except Exception as e:
                logger.error(f"Error in realtime resampling: {str(e)}", exc_info=True)
                time.sleep(self.SLEEP_DURATION['ERROR'])


def main():
    """Main function to run the data processing"""
    import argparse

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Real-time data processing')
    parser.add_argument(
        '--type', 
        type=str,
        choices=['stock', 'ps'],
        required=True,
        help='Type of data to process: stock or ps'
    )
    parser.add_argument(
        '--timeframe',
        type=str,
        default='30S',
        help='Timeframe for data resampling (default: 30S)'
    )
    
    args = parser.parse_args()
    
    # Get current day
    current_day = datetime.now().strftime("%Y_%m_%d")
    
    # Configure processor based on type argument
    if args.type == 'stock':
        processor = ProcessStockData
        output_key = 'pslab_realtime_stockdata2'
    else:  # ps
        processor = ProcessPsData
        output_key = 'pslab_realtime_psdata2'
    
    # Log start of process
    logger.info(f"Starting {args.type} data processing with {args.timeframe} timeframe")
    
    # Create and start aggregator
    aggregator = Aggregator(
        day=current_day,
        timeframe=args.timeframe,
        output_plasma_key=output_key,
        data_processor=processor
    )
    
    aggregator.start_realtime_resampling()


if __name__ == "__main__" and not check_run_with_interactive():
    main()