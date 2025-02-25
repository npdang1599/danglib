import pandas as pd
import numpy as np
from redis import StrictRedis, ConnectionPool
from datetime import datetime, time
import time as time_module
from typing import Dict, List, Optional, Union
from dataclasses import dataclass
from functools import lru_cache
from danglib.pslab.logger_module import DataLogger
logger = DataLogger("/home/ubuntu/Dang/project_ps/logs")
from danglib.pslab.resources import Globs
from danglib.lazy_core import gen_plasma_functions


# Constants
@dataclass(frozen=True)
class TradingTime:
    MORNING_START: time = time(9, 15)
    LUNCH_START: time = time(11, 30)
    LUNCH_END: time = time(13, 0)
    AFTERNOON_END: time = time(14, 50)

class ProcessData:
    # Use TypedDict for better type hints
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

    class Libs:
        @staticmethod
        @lru_cache(maxsize=128)
        def time_to_seconds(time_str: str) -> int:
            """Convert time string to seconds with caching for better performance"""
            h, m, s = map(int, time_str.split(':'))
            return h * 3600 + m * 60 + s

        @staticmethod
        def calculate_foreign_fubon(
            dfi: pd.DataFrame,
            put_through_data: Optional[pd.DataFrame] = None,
            tolerance: float = 0.002
        ) -> pd.DataFrame:
            """Optimized foreign transactions calculation"""
            try:
                df = dfi.copy()
                if df.empty:
                    logger.log('WARNING', "Empty input dataframe for foreign calculations")
                    return pd.DataFrame()

                # Use vectorized operations instead of groupby where possible
                df['fBuyVol'] = df.groupby('stock')['foreignerBuyVolume'].diff().fillna(df['foreignerBuyVolume'])
                df['fSellVol'] = df.groupby('stock')['foreignerSellVolume'].diff().fillna(df['foreignerSellVolume'])
                
                if put_through_data is not None and not put_through_data.empty:
                    # Process put-through data more efficiently
                    df_tt = (put_through_data[put_through_data['stockSymbol'].isin(Globs.STOCKS)]
                            .assign(time=lambda x: x['createdAt'].apply(ProcessData.Libs.time_to_seconds))
                            .groupby(['stockSymbol', 'time'])['vol']
                            .sum()
                            .reset_index()
                            .rename(columns={'stockSymbol': 'stock'})
                            .query('time <= 53159'))

                    # Efficient merge
                    df = pd.merge_asof(
                        df.sort_values('time'),
                        df_tt,
                        by='stock',
                        on='time',
                        tolerance=15,
                        direction='nearest'
                    )
                else:
                    df['vol'] = np.nan

                # Vectorized calculations
                df['fBuyVal'] = df['fBuyVol'] * df['close']
                df['fSellVal'] = df['fSellVol'] * df['close']
                
                # Efficient put-through masking
                pt_mask = ((df['fBuyVol'].between(df['vol'] * (1 - tolerance), df['vol'] * (1 + tolerance))) |
                          (df['fSellVol'].between(df['vol'] * (1 - tolerance), df['vol'] * (1 + tolerance))))
                
                return df[~pt_mask][['stock', 'time', 'fBuyVal', 'fSellVal']]
            
            except Exception as e:
                logger.log('ERROR', f"Error in calculate_foreign_fubon: {str(e)}")
                return pd.DataFrame()

        @staticmethod
        def calculate_matching_value(df: pd.DataFrame) -> pd.DataFrame:
            """Optimized matching value calculation"""
            if 'matchingValue' not in df.columns:
                if 'matchingVolume' in df.columns:
                    df['matchingValue'] = df['matchingVolume'] * df['last']
                else:
                    sorted_df = df.sort_values(['timestamp'])
                    if 'totalMatchValue' in df.columns:
                        df['matchingValue'] = sorted_df.groupby('stock')['totalMatchValue'].diff()
                    elif 'totalMatchVolume' in df.columns:
                        df['matchingValue'] = sorted_df.groupby('stock')['totalMatchVolume'].diff() * sorted_df['last']
                    else:
                        raise ValueError("Cannot calculate `matchingValue`")
            
            df['matchingValue'] = np.maximum(df['matchingValue'], 0) / 1e9
            return df[['stock', 'timestamp', 'matchingValue']]

        @staticmethod
        def calculate_bidask(df: pd.DataFrame) -> None:
            """Optimized bid/ask calculation"""
            bid_cols = ['bestBid1', 'bestBid2', 'bestBid3']
            bid_vol_cols = ['bestBid1Volume', 'bestBid2Volume', 'bestBid3Volume']
            offer_cols = ['bestOffer1', 'bestOffer2', 'bestOffer3']
            offer_vol_cols = ['bestOffer1Volume', 'bestOffer2Volume', 'bestOffer3Volume']

            df['bid'] = (df[bid_cols] * df[bid_vol_cols]).sum(axis=1) / 1e9
            df['ask'] = (df[offer_cols] * df[offer_vol_cols]).sum(axis=1) / 1e9

@dataclass
class TimeRange:
    start: int
    end: int

class Resampler:
    # Constants optimized for quick access
    TIME_CONSTANTS = {
        'MORNING': {'START': 33300, 'END': 41400},  # 9:15 and 11:30
        'AFTERNOON': {'START': 46800, 'END': 52200},  # 13:00 and 14:30
        'ATC': 53100  # 14:45
    }

    @staticmethod
    @lru_cache(maxsize=32)
    def get_agg_dict(names: tuple) -> dict:
        """Cached aggregation dictionary generator"""
        if len(names[0]) == 2:
            return {n: Globs.STANDARD_AGG_DIC[n[0]] for n in names}
        return {n: v for n, v in Globs.STANDARD_AGG_DIC.items() if n in names}

    @classmethod
    @staticmethod
    def _create_time_filter(df: pd.DataFrame, time_range: TimeRange) -> pd.Series:
        """Create time filter for a specific trading session"""
        return (df['time'] >= time_range.start) & (df['time'] < time_range.end)

    @classmethod
    def cleanup_timestamp(cls, day: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean up timestamps in the dataframe based on trading sessions
        
        Args:
            day: Trading day in YYYY_MM_DD format
            df: Input dataframe with 'time' and 'timestamp' columns
            
        Returns:
            DataFrame with cleaned timestamps
        """
        df = df.copy()
        tc = cls.TIME_CONSTANTS
        
        # Create session filters
        morning = TimeRange(tc['MORNING']['START'], tc['MORNING']['END'])
        afternoon = TimeRange(tc['AFTERNOON']['START'], tc['AFTERNOON']['END'])
        
        filters = {
            'morning': cls._create_time_filter(df, morning),
            'afternoon': cls._create_time_filter(df, afternoon),
            'atc': df['time'] >= tc['ATC']
        }
        
        # Apply trading session filters
        valid_time_mask = filters['morning'] | filters['afternoon'] | filters['atc']
        df = df[valid_time_mask].copy()
        
        # Set ATC timestamps
        atc_mask = filters['atc']
        if atc_mask.any():
            df.loc[atc_mask, 'time'] = tc['ATC']
            atc_timestamp = int(pd.to_datetime(f"{day} 14:45:00").timestamp())
            df.loc[atc_mask, 'timestamp'] = atc_timestamp
        
        return df

    @classmethod
    def _calculate_candletime(cls, timestamps: pd.Series, timeframe: str) -> pd.Series:
        """
        Calculate candle timestamps based on timeframe
        
        Args:
            timestamps: Series of Unix timestamps
            timeframe: Timeframe string (e.g., '30S', '1M', '5M')
            
        Returns:
            Series of calculated candle timestamps
        """
        tc = cls.TIME_CONSTANTS
        
        # Calculate base timestamps
        day_start = (timestamps // 86400) * 86400
        seconds_in_day = timestamps % 86400
        morning_start = day_start + tc['MORNING']['START']
        afternoon_start = day_start + tc['AFTERNOON']['START']
        
        # Convert timeframe to seconds
        tf_seconds = Globs.TF_TO_MIN.get(timeframe, 1) * 60
        
        # Calculate trading day start (9:00)
        trading_start = day_start + 9 * 3600
        
        # Calculate base candle times
        new_candles = ((timestamps - trading_start) // tf_seconds) * tf_seconds + trading_start
        new_candles_time = new_candles % 86400
        
        # Adjust timestamps for session breaks
        morning_mask = new_candles_time <= tc['MORNING']['START']
        lunch_mask = new_candles_time.between(tc['MORNING']['END'], tc['AFTERNOON']['START'])
        
        new_candles = pd.Series(
            np.select(
                [morning_mask, lunch_mask],
                [morning_start, afternoon_start],
                new_candles
            ),
            index=timestamps.index
        )
        
        # Handle ATC period for intraday timeframes
        if timeframe != '1D':
            atc_mask = seconds_in_day == tc['ATC']
            new_candles = pd.Series(
                np.where(atc_mask, timestamps, new_candles),
                index=timestamps.index
            )
        
        return new_candles

    def transform(cls, 
                 day: str, 
                 df: pd.DataFrame, 
                 timeframe: str, 
                 cleanup_data: bool = False, 
                 subgroups: Optional[List[str]] = None) -> pd.DataFrame:
        """Optimized data transformation"""
        if cleanup_data:
            df = cls.cleanup_timestamp(day, df)

        df['candleTime'] = cls._calculate_candletime(df['timestamp'], timeframe)
        
        groups = ['candleTime']
        if subgroups:
            groups.extend(subgroups)

        # Convert column names to tuple for caching
        agg_dict = cls.get_agg_dict(tuple(df.columns))
        
        return df.groupby(groups).agg(agg_dict).reset_index()

class Aggregator:
    # Sleep durations in seconds
    SLEEP_DURATION = {
        'OUTSIDE_TRADING': 60,
        'EMPTY_DATA': 1,
        'ERROR': 1
    }
    def __init__(self, 
                 day: str, 
                 timeframe: str, 
                 output_plasma_key: str, 
                 current_pos: Optional[int] = None,
                 batch_size: int = 1000):
        self.day = day
        self.timeframe = timeframe
        self.timeframe_seconds = Globs.TF_TO_MIN.get(timeframe) * 60
        self.output_key = f"{output_plasma_key}.{timeframe}"
        self.current_pos = current_pos or 0
        self.batch_size = batch_size
        self.initialize_redis_connections()
        
    def initialize_redis_connections(self):
        """Initialize Redis connections with connection pooling"""
        self.redis_pool = StrictRedis(
            connection_pool=ConnectionPool(
                max_connections=10,
                decode_responses=True
            )
        )
        
    def start_realtime_resampling(self):
        """Optimized realtime resampling process"""
        while True:
            try:
                if not self._is_trading_time():
                    time_module.sleep(self.SLEEP_DURATION['OUTSIDE_TRADING'])
                    continue

                raw_data = self._load_batch_data()
                if raw_data.empty:
                    time_module.sleep(self.SLEEP_DURATION['EMPTY_DATA'])
                    continue

                processed_data = self._process_batch(raw_data)
                if not processed_data.empty:
                    self._update_resampled_data(processed_data)
                    self._store_data()

                sleep_duration = self._calculate_next_cycle_wait()
                if sleep_duration > 0:
                    time_module.sleep(sleep_duration)

            except Exception as e:
                logger.log('ERROR', f"Resampling error: {str(e)}")
                time_module.sleep(self.SLEEP_DURATION['ERROR'])

    def _load_batch_data(self) -> pd.DataFrame:
        """Load data in batches for better memory management"""
        try:
            return self.data_loader(
                self.redis_pool, 
                self.day, 
                self.current_pos, 
                self.current_pos + self.batch_size
            )
        except Exception as e:
            logger.log('ERROR', f"Redis load error: {str(e)}")
            return pd.DataFrame()

    def _is_trading_time(self) -> bool:
        """Check if current time is within trading hours"""
        current_time = datetime.now().time()
        
        if current_time < TradingTime.MORNING_START or current_time > TradingTime.AFTERNOON_END:
            return False
            
        if TradingTime.LUNCH_START <= current_time <= TradingTime.LUNCH_END:
            return False
            
        return True

    def _get_candle_stamp(self, timestamp: int) -> int:
        """Calculate the start timestamp of the current candle"""
        return (timestamp // self.timeframe_seconds) * self.timeframe_seconds
    
    def _get_next_candle_stamp(self, current_timestamp: int) -> int:
        """Calculate the start timestamp of the next candle"""
        current_candle = self._get_candle_stamp(current_timestamp)
        return current_candle + self.timeframe_seconds
    
    def _calculate_next_cycle_wait(self) -> float:
        """Calculate wait time until next processing cycle"""
        current_stamp = time_module.time() + 7 * 3600
        next_candle = self._get_next_candle_stamp(current_stamp)
        return max(0, next_candle - current_stamp)

    def _add_empty_candle(self, candlestamp: int, df: pd.DataFrame) -> pd.DataFrame:
        """Add empty candles for all stocks at given timestamp"""
        new_rows = []
        
        for stock in df['stock'].unique():
            new_row = pd.Series(
                index=df.columns,
                data=np.nan,
                name=len(df)
            )
            new_row['candleTime'] = candlestamp
            new_row['stock'] = stock
            new_rows.append(new_row)
        
        if new_rows:
            return pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
        return df

    def _update_last_states(self, df: pd.DataFrame) -> None:
        """Update last known states for each stock"""
        for stock in df['code'].unique():
            stock_data = df[df['code'] == stock].iloc[-1]
            self.last_states[stock] = {
                'buyForeignQtty': stock_data['buyForeignQtty'],
                'sellForeignQtty': stock_data['sellForeignQtty'],
                'lastPrice': stock_data['lastPrice'],
                'timestamp': stock_data['timestamp']
            }

    def _append_last_states(self, df: pd.DataFrame) -> pd.DataFrame:
        """Append last known states to the dataframe"""
        if not self.last_states:
            return df
            
        last_states_df = pd.DataFrame(list(self.last_states.values()))
        return pd.concat([last_states_df, df]).sort_values(['code', 'timestamp'])

    def _update_resampled_data(self, processed_data: pd.DataFrame) -> None:
        """Update resampled data with new processed data"""
        new_resampled = Resampler.transform(
            self.day,
            processed_data,
            self.timeframe,
            cleanup_data=True,
            subgroups=['stock']
        )
        
        if self.resampled_df.empty:
            self.resampled_df = new_resampled
        else:
            self.resampled_df = self.merge_resampled_data(
                self.resampled_df,
                new_resampled,
                Resampler.get_agg_dict(tuple(self.resampled_df.columns))
            )
        
        self.resampled_df = ProcessData.postprocess_realtime_stock_data(self.resampled_df)

    def _store_data(self) -> None:
        """Store processed data to Plasma database"""
        try:
            _, disconnect, psave, _ = gen_plasma_functions(db=Globs.PLASMA_DB)
            psave(self.output_key, self.resampled_df)
        except Exception as e:
            logger.log('ERROR', f"Failed to store data: {str(e)}")

    def _process_batch(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """Process a batch of data with optimized operations"""
        if raw_data.empty:
            return pd.DataFrame()

        with_states = self._append_last_states(raw_data)
        processed = self.preprocessor(with_states, self.day)
        self._update_last_states(raw_data)
        
        return processed[processed['timestamp'] >= self.current_candle_stamp]