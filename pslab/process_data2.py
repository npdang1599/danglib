#%%
import pandas as pd
import numpy as np
from redis import StrictRedis
from datetime import datetime
import time

from danglib.utils import check_run_with_interactive
from danglib.pslab.utils import Utils
from danglib.pslab.resources import Adapters, Globs
from danglib.lazy_core import gen_plasma_functions
from danglib.pslab.logger_module import DataLogger
logger = DataLogger("/home/ubuntu/Dang/project_ps/logs")

r = StrictRedis(decode_responses=True)
rlv2 = StrictRedis(host='lv2', decode_responses=True)

class ProcessData:

    REALTIME_HOSE500_COLUMNS_MAPPING = {
            'code': 'stock', 
            'lastPrice': 'close',
            'lastVol': 'volume',
            'buyForeignQtty':'foreignerBuyVolume',
            'sellForeignQtty':'foreignerSellVolume',
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
        def calculate_foreign_fubon(
            dfi: pd.DataFrame,
            put_through_data: pd.DataFrame = None,
            tolerance: float = 0.002
        ) -> pd.DataFrame:
            """
            Calculate foreign transactions for Fubon stocks with optimized performance.
            
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
            def test():
                day = "2025_02_18"
                put_through_data = Adapters.load_thoathuan_dc_data_from_db(day)
                df_raw = Adapters.RedisAdapters.load_realtime_stock_data_from_redis(r, day, 0, 100000)
                dfi = ProcessData.preprocess_realtime_stock_data(df_raw)

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
                df['fBuyVol'] = df.groupby('stock')['foreignerBuyVolume'].diff().fillna(df['foreignerBuyVolume'])
                df['fSellVol'] = df.groupby('stock')['foreignerSellVolume'].diff().fillna(df['foreignerSellVolume'])

                df = df.dropna(subset=['fBuyVol', 'fSellVol'])
                
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
                logger.log('ERROR', f"Error in calculate_foreign_fubon: {str(e)}")
                return pd.DataFrame()

        @staticmethod
        def calculate_matching_value(df: pd.DataFrame):
        
            def diff(df: pd.DataFrame, column_name):
                df = df.copy()
                df = df.sort_values(['timestamp', column_name])
                df['matchingValue'] = df.groupby('stock')[column_name].diff()
                df.dropna(subset=[column_name], inplace=True)
                return df

            if 'matchingValue' not in df.columns:
                if 'matchingVolume' in df.columns:
                    df['matchingValue'] = df['matchingVolume'] * df['last']
                elif 'totalMatchValue' in df.columns:
                    df = diff(df, 'totalMatchValue')
                elif 'totalMatchVolume' in df.columns:
                    df = diff(df, 'totalMatchVolume')
                    df['matchingValue'] = df['matchingValue'] * df['last']
                else:
                    raise ValueError("Can not calculate `matchingValue`")
            
            df['matchingValue'] = np.where(df['matchingValue'] < 0, 0, df['matchingValue']) / 1e9
    
            return df[['stock', 'timestamp', 'matchingValue']]

        @staticmethod
        def calculate_busd(
            df: pd.DataFrame
        ):
            df['price_diff'] = df.groupby('stock')['close'].diff()
            df = df.dropna(subset=['price_diff'])
            bu_cond = (df['price_diff'] > 0)
            sd_cond = (df['price_diff'] < 0)
            df['matchedBy2'] = np.where(bu_cond, 1, np.where(sd_cond, -1, np.NaN))
            df['matchedBy2'] = df.groupby('stock')['matchedBy2'].ffill()
            df['matchedBy2'] = df['matchedBy2'].fillna(0)

            if 'matchedBy' not in df.columns:   
                df['matchedBy'] = df['matchedBy2']

            # Compute BU SD base on existed `matchedBy` column
            df['bu'] = np.where(df['matchedBy'] == 1, df['matchingValue'], 0)
            df['sd'] = np.where(df['matchedBy'] == -1, df['matchingValue'], 0)

            # Compute BU SD base on re-calculated `matchedBy2` column
            df['bu2'] = np.where(df['matchedBy2'] == 1, df['matchingValue'], 0)
            df['sd2'] = np.where(df['matchedBy2'] == -1, df['matchingValue'], 0)

            df.drop(['matchedBy2', 'matchedBy'], axis=1, inplace=True)

        @staticmethod
        def calculate_bidask(df: pd.DataFrame):
            # Calculate Bid/Ask
            df['bid'] = (
                df['bestBid1'] * df['bestBid1Volume'] +\
                df['bestBid2'] * df['bestBid2Volume'] +\
                df['bestBid3'] * df['bestBid3Volume']
            ) / 1e9

            df['ask'] = (
                df['bestOffer1'] * df['bestOffer1Volume'] +\
                df['bestOffer2'] * df['bestOffer2Volume'] +\
                df['bestOffer3'] * df['bestOffer3Volume']
            ) / 1e9

    @staticmethod
    def ensure_all_stocks_present(df: pd.DataFrame, required_stocks: list) -> pd.DataFrame:
        """
        Đảm bảo tất cả stocks có mặt trong DataFrame. Nếu thiếu stock nào thì thêm dòng với giá trị NaN.
        
        Args:
            df (pd.DataFrame): DataFrame gốc
            required_stocks (list): Danh sách các stocks cần có
            
        Returns:
            pd.DataFrame: DataFrame đã được bổ sung đầy đủ stocks
        """
        try:
            if df.empty:
                logger.log('WARNING', "Input DataFrame is empty in ensure_all_stocks_present")
                return df
                
            # Lấy timestamp lớn nhất
            max_timestamp = df['timestamp'].max()
            max_time = df['time'].max()
            
            # Kiểm tra các stocks hiện có
            existing_stocks = set(df['stock'].unique())
            required_stocks_set = set(required_stocks)
            
            # Tìm các stocks bị thiếu
            missing_stocks = required_stocks_set - existing_stocks
            
            if missing_stocks:
                logger.log('WARNING', f"Missing stocks in data: {missing_stocks}")
                
                # Tạo template cho dòng dữ liệu mới
                template_row = pd.Series(
                    index=df.columns,
                    data=np.nan
                )
                
                # Tạo các dòng mới cho stocks bị thiếu
                new_rows = []
                for stock in missing_stocks:
                    new_row = template_row.copy()
                    new_row['stock'] = stock
                    new_row['timestamp'] = max_timestamp
                    new_row['time'] = max_time
                    new_rows.append(new_row)
                
                # Thêm các dòng mới vào DataFrame
                if new_rows:
                    df_missing = pd.DataFrame(new_rows)
                    df = pd.concat([df, df_missing], ignore_index=True)
                    logger.log('INFO', f"Added {len(new_rows)} rows for missing stocks")
            
            return df
            
        except Exception as e:
            logger.log('ERROR', f"Error in ensure_all_stocks_present: {str(e)}")
            return df

    @staticmethod
    def preprocess_realtime_stock_data(df: pd.DataFrame, day):

        def test():
            day = "2025_02_20"
            df = Adapters.RedisAdapters.load_realtime_stock_data_from_redis(r, day, 0, -1)
            df_columns = ['timestamp', 'code', 'best1Bid', 'best1BidVol', 'best2Bid',
                            'best2BidVol', 'best3Bid', 'best3BidVol', 'best1Offer', 'best1OfferVol',
                            'best2Offer', 'best2OfferVol', 'best3Offer', 'best3OfferVol',
                            'lastPrice', 'lastVol', 'highestPrice', 'exchange', 'lowestPrice',
                            'avgPrice', 'buyForeignQtty', 'sellForeignQtty', 'priceChange',
                            'priceChangePercent', 'totalMatchVol', 'totalMatchValue',
                            'ceilingPrice', 'floorPrice', 'refPrice', 'session', 'openPrice',
                            'closePrice', 'symbol', 'source']

        try:
            if df.empty:
                logger.log('WARNING', "Empty input dataframe in preprocessing")
                return pd.DataFrame()
            

            # Validate required columns
            required_cols = list(ProcessData.REALTIME_HOSE500_COLUMNS_MAPPING.keys())
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.log('ERROR', f"Missing columns in input data: {missing_cols}")
                return pd.DataFrame()
            

            df = df[df['code'].isin(Globs.STOCKS) & (df['lastPrice'] != 0)].copy()
            logger.log('INFO', f"Processing {len(df)} valid records after filtering")
        
            df['timestamp'] = df['timestamp'] // 1000 + 7*60*60
            df['time'] = df['timestamp'] % 86400

            df = df.rename(columns=ProcessData.REALTIME_HOSE500_COLUMNS_MAPPING)
            df = ProcessData.ensure_all_stocks_present(df, Globs.STOCKS)
            df = df.sort_values(['timestamp', 'totalMatchVolume'])

            if 'matchingValue' not in df.columns:
                df_value = ProcessData.Libs.calculate_matching_value(df)
                df = df.merge(df_value, how='left', on=['stock', 'timestamp'])
                logger.log('INFO', f"Created matching value: {df_value.columns}, {df_value.shape}")

            df['open'] = df['high'] = df['low'] = df['close'].copy()

            # ProcessData.Libs.calculate_busd(df)
            df['price_diff'] = df.groupby('stock')['close'].diff()
            df = df.dropna(subset=['price_diff'])
            bu_cond = (df['price_diff'] > 0)
            sd_cond = (df['price_diff'] < 0)
            df['matchedBy2'] = np.where(bu_cond, 1, np.where(sd_cond, -1, np.NaN))
            df['matchedBy2'] = df.groupby('stock')['matchedBy2'].ffill()
            df['matchedBy2'] = df['matchedBy2'].fillna(0)

            if 'matchedBy' not in df.columns:   
                df['matchedBy'] = df['matchedBy2']

            # Compute BU SD base on existed `matchedBy` column
            df['bu'] = np.where(df['matchedBy'] == 1, df['matchingValue'], 0)
            df['sd'] = np.where(df['matchedBy'] == -1, df['matchingValue'], 0)

            # Compute BU SD base on re-calculated `matchedBy2` column
            df['bu2'] = np.where(df['matchedBy2'] == 1, df['matchingValue'], 0)
            df['sd2'] = np.where(df['matchedBy2'] == -1, df['matchingValue'], 0)

            df.drop(['matchedBy2', 'matchedBy'], axis=1, inplace=True)

            ProcessData.Libs.calculate_bidask(df)

            agg_dic = {
                'timestamp': 'last', 
                'open': 'first', 
                'high': 'max', 
                'low': 'min', 
                'close': 'last', 
                'matchingValue': 'sum',
                'foreignerBuyVolume': 'last', 
                'foreignerSellVolume': 'last',
                'bu': 'sum', 
                'sd': 'sum',
                'bu2': 'sum',
                'sd2': 'sum', 
                'bid': 'last', 
                'ask': 'last',
                'refPrice': 'last'
            }

            # 1. Group data theo giây trước
            # Group data by stock and time (seconds) first
            df = df.groupby(['stock', 'time'], as_index=False).agg(agg_dic).reset_index()

            # 2. Tính toán foreign_fubon (vẫn có laststate)
            dftt = Adapters.load_put_through_data_realtime(rlv2, day)
            df_foreign = ProcessData.Libs.calculate_foreign_fubon(df, dftt)
            df = df.merge(df_foreign, how='left', on=['stock', 'time'])
            df['fBuyVal'] = df['fBuyVal'].fillna(0)
            df['fSellVal'] = df['fSellVal'].fillna(0)

            # 3. Sau khi tính xong mới loại bỏ laststate
            min_timestamps = df.groupby('stock')['timestamp'].min()
            non_laststate_mask = df.apply(lambda row: row['timestamp'] > min_timestamps[row['stock']], axis=1)
            df = df[non_laststate_mask]

            required_sources = [i for i in agg_dic.keys() if i not in ['foreignerBuyVolume', 'foreignerSellVolume']]
            required_sources.extend(['stock', 'time', 'fBuyVal', 'fSellVal'])

            return df[required_sources]
        
        except Exception as e:
            logger.log('ERROR', f"Error in preprocess_realtime_stock_data: {str(e)}")
            return pd.DataFrame()
    
    @staticmethod
    def postprocess_realtime_stock_data(df: pd.DataFrame):
        """
        Hàm xử lý sau khi resample với dữ liệu đã pivot
        """
        # Điền các giá trị còn thiếu
        df['refPrice'] = df['refPrice'].ffill().bfill()
        df['close'] = df['close'].ffill()
        df['bid'] = df['bid'].ffill().fillna(0)
        df['ask'] = df['ask'].ffill().fillna(0)
        df['matchingValue'] = df['matchingValue'].fillna(0)
        df['bu'] = df['bu'].fillna(0)
        df['sd'] = df['sd'].fillna(0)
        df['bu2'] = df['bu2'].fillna(0)
        df['sd2'] = df['sd2'].fillna(0)
        df['fBuyVal'] = df['fBuyVal'].fillna(0)
        df['fSellVal'] = df['fSellVal'].fillna(0)

        # Fill các giá trị OHLC
        df['close'] = df['close'].fillna(df['refPrice'])
        df['open'] = df['open'].fillna(df['close'])
        df['high'] = df['high'].fillna(df['close'])
        df['low'] = df['low'].fillna(df['close'])

        return df


class Resampler:
    # Constants for time calculations
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
    def get_agg_dict(names: list):
        
        if len(names[0]) == 2:
            return {n: Globs.STANDARD_AGG_DIC[n[0]] for n in names}
        else:
            return {n: v for n, v in Globs.STANDARD_AGG_DIC.items() if n in names}
    
    @staticmethod
    def _create_time_filter(df: pd.DataFrame, time_range: dict) -> pd.Series:
        """Create time filter for a specific trading session"""
        return (df['time'] >= time_range['START']) & (df['time'] < time_range.get('END', float('inf')))

    @classmethod
    def cleanup_timestamp(cls, day, df: pd.DataFrame) -> pd.DataFrame:
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
        
        return df

    @classmethod
    def _calculate_candletime(cls, timestamps: pd.Series, timeframe: str) -> pd.Series:
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
    def transform(cls, day, df: pd.DataFrame, timeframe: str, cleanup_data: bool = False, subgroup=None):
        df = df.copy()
        if cleanup_data:
            df = cls.cleanup_timestamp(day, df)
        
        # Calculate new candle times
        df['candleTime'] = cls._calculate_candletime(df['timestamp'], timeframe)

        groups = ['candleTime']
        if subgroup:
            groups.append(subgroup)

        df = df.groupby(by=groups).agg(cls.get_agg_dict(list(df.columns))).reset_index()
        df = df.drop('timestamp', axis=1)

        if subgroup:
            df = df.pivot(index='candleTime', columns=subgroup)

        return df

class Aggregator:
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

    def __init__(self, day, timeframe, output_plasma_key, current_pos=None):
        self.day = day
        self.timeframe = timeframe
        self.timeframe_seconds = Globs.TF_TO_MIN.get(timeframe) * 60
        self.r = StrictRedis(decode_responses=True)
        self.data_loader = Adapters.RedisAdapters.load_realtime_stock_data_from_redis
        self.preprocessor = ProcessData.preprocess_realtime_stock_data
        self.postprocessor = ProcessData.postprocess_realtime_stock_data
        self.output_key = f"{output_plasma_key}.{timeframe}"

        self.current_pos = 0 if current_pos is None else current_pos
        self.current_candle_stamp = 0
        self.finished_candle_stamp = 0
        self.resampled_df = pd.DataFrame()
        self.last_states = {}  # Dict lưu trạng thái cuối của mỗi mã

    @staticmethod
    def join_historical_data(df:pd.DataFrame, ndays:int=10):
        from_day = Adapters.get_historical_day_by_index(-ndays)
        logger.log('INFO', f'Starting to merge historical data from {from_day}...')

        from_stamp = Utils.day_to_timestamp(from_day)
        df_his = Adapters.load_stock_data_from_plasma()
        
        df_his = df_his[df_his.index >= from_stamp]
        df_his.index = df_his.index // 1e9
        df_his = df_his.drop(['accNetBusd', 'accNetBusd2', 'return'], axis=1, level=0)
        df_merge = pd.concat([df_his, df])
        df_merge = df_merge.sort_index(ascending=True)

        logger.log('INFO', f'Successfully merged historical data. After merge shape: {df.shape}')
        return df_merge

    def load_redis_data(self, start, end):
        try:
            df = self.data_loader(self.r, self.day, start, end)
            logger.log('DEBUG', f"Loaded data from Redis: {len(df)} records")
            return df
        except Exception as e:
            logger.log('ERROR', f"Redis connection error: {str(e)}")
            return pd.DataFrame()

    def _get_next_candle_stamp(self, current_timestamp):
        """Tính thời điểm bắt đầu của nến tiếp theo"""
        current_candle = self._get_candle_stamp(current_timestamp)
        return current_candle + self.timeframe_seconds
    
    def _get_candle_stamp(self, timestamp):
        """Tính thời điểm bắt đầu của nến hiện tại"""
        return (timestamp // self.timeframe_seconds) * self.timeframe_seconds
    
    def _get_finished_candle_stamp(self, timestamp):
        """Tính thời điểm bắt đầu của nến gần nhất đã hoàn thành"""
        current_candle = self._get_candle_stamp(timestamp)
        return current_candle - self.timeframe_seconds

    def _add_empty_candle(self, candlestamp, df: pd.DataFrame):
        """Tạo nến trống với giá trị NaN cho dữ liệu đã pivot"""
        # Tạo một DataFrame mới với index là candlestamp và các cột giống df
        new_df = pd.DataFrame(
            index=[candlestamp],
            columns=df.columns,
            data=np.nan
        )
        
        # Concat với DataFrame cũ
        return pd.concat([df, new_df])

    def _update_last_states(self, df: pd.DataFrame):
        """Cập nhật trạng thái cuối của mỗi mã"""
        for stock in df['code'].unique():
            stock_data = df[df['code'] == stock].iloc[-1]
            self.last_states[stock] = {
                'buyForeignQtty': stock_data['buyForeignQtty'],
                'sellForeignQtty': stock_data['sellForeignQtty'],
                'lastPrice': stock_data['lastPrice'],
                'timestamp': stock_data['timestamp']
            }

    def _append_last_states(self, df: pd.DataFrame) -> pd.DataFrame:
        """Thêm last states vào đầu DataFrame mới để tính diff chính xác"""
        last_states_rows = []
        for stock in df['code'].unique():
            if stock in self.last_states:
                last_states_rows.append(self.last_states[stock])
        
        if last_states_rows:
            last_states_df = pd.DataFrame(last_states_rows)
            return pd.concat([last_states_df, df]).sort_values(['code', 'timestamp'])
        return df

    @staticmethod
    def merge_resampled_data(old_df: pd.DataFrame, new_df: pd.DataFrame, agg_dict: dict) -> pd.DataFrame:
        """
        Merge và tính toán lại các giá trị cho các candles trùng nhau
        dựa trên agg_dict với dữ liệu đã pivot
        """
        if old_df.empty:
            return new_df
            
        # Vì data đã pivot nên index sẽ là candleTime
        common_idx = old_df.index.intersection(new_df.index)
        
        if not common_idx.empty:
            # Gộp data cho các candles trùng nhau
            overlap_data = pd.concat([
                old_df.loc[common_idx],
                new_df.loc[common_idx]
            ])

            # Tính toán lại theo agg_dict cho từng cột
            # Lấy tên các cột gốc từ MultiIndex
            columns = overlap_data.columns.get_level_values(0).unique()
            
            recalculated = pd.DataFrame(index=common_idx)
            for col in columns:
                # Lấy phương thức aggregation cho cột
                agg_method = agg_dict.get(col, 'last')  # Default to 'last' if not specified
                col_data = overlap_data[col].groupby(level=0).agg(agg_method)
                recalculated = pd.concat([recalculated, col_data], axis=1)

            # Combine all data
            result_df = pd.concat([
                old_df.loc[old_df.index.difference(common_idx)],  # Rows only in old
                recalculated,                                     # Recalculated rows
                new_df.loc[new_df.index.difference(common_idx)]   # Rows only in new
            ]).sort_index()
        else:
            # Nếu không có overlap, đơn giản là concat
            result_df = pd.concat([old_df, new_df]).sort_index()
        
        return result_df
    
    def _store_resampled_data(self, df: pd.DataFrame):
        _, disconnect, psave, pload = gen_plasma_functions(db=Globs.PLASMA_DB)
        psave(self.output_key, df)

    def start_realtime_resampling(self):
        """Bắt đầu quá trình resample real-time"""
        logger.log('INFO', f"Starting realtime resampling with timeframe: {self.timeframe}")

        while True:
            try:
                current_time = datetime.now()
                time_str = current_time.strftime('%H:%M:%S')
                current_time = current_time.time()

                # Skip nếu ngoài giờ giao dịch
                if not (self.TRADING_HOURS['MORNING_START'] <= current_time <= self.TRADING_HOURS['AFTERNOON_END']):
                    logger.log('INFO', f"Outside trading hours ({current_time})")
                    time.sleep(self.SLEEP_DURATION['OUTSIDE_TRADING'])
                    continue

                # Skip trong giờ nghỉ trưa
                if self.TRADING_HOURS['LUNCH_START'] <= current_time <= self.TRADING_HOURS['LUNCH_END']:
                    print(f"[{time_str}] Lunch break ({current_time})")
                    time.sleep(self.SLEEP_DURATION['OUTSIDE_TRADING'])
                    continue

                # Load dữ liệu mới từ Redis
                raw_data = self.load_redis_data(self.current_pos, -1)
                logger.log('INFO', f"Processing data from position {self.current_pos}")
                self.current_pos += len(raw_data) + 1
                logger.log('INFO', f"Updated current_pos to {self.current_pos}")
            
                if not raw_data.empty:

                    # Thêm last states vào trước khi xử lý
                    raw_data_with_states = self._append_last_states(raw_data)

                    # Preprocess dữ liệu trước khi resample
                    new_data = self.preprocessor(raw_data_with_states, self.day)

                    # Lọc dữ liệu từ candle hiện tại
                    new_data = new_data[new_data['timestamp'] >= self.current_candle_stamp]
                    self.current_candle_stamp = self._get_candle_stamp(new_data['timestamp'].max())
                    logger.log('INFO', f"Updated current_candle_stamp to {self.current_candle_stamp}")

                    # Cập nhật last states cho lần sau
                    self._update_last_states(raw_data)

                    if not new_data.empty:
                        # Resample dữ liệu đã preprocess
                        new_resampled = Resampler.transform(
                            self.day,
                            new_data,
                            self.timeframe,
                            cleanup_data=True,
                            subgroup = 'stock'
                        )
                        print(f"New resampled data shape: {new_resampled.info()}")
                        # logger.log('INFO', f"New resampled data columns: {new_resampled.columns}")

                        if self.resampled_df.empty:
                            self.resampled_df = self.join_historical_data(new_resampled, ndays=10)
                            print(f"Init resampled df shape: {self.resampled_df.info()}")

                        else:
                            # Merge với dữ liệu cũ và tính toán lại theo agg_dict
                            self.resampled_df = self.merge_resampled_data(
                                self.resampled_df, 
                                new_resampled,
                                agg_dict = Resampler.get_agg_dict(list(self.resampled_df.columns))
                            )

                            print(f"After merge shape: {self.resampled_df.shape}")

                        self.resampled_df = self.postprocessor(self.resampled_df)

                candle_stamp = max(self.resampled_df.index)
                if candle_stamp == self.current_candle_stamp:
                    candle_stamp = self._get_next_candle_stamp(candle_stamp)
                    self._add_empty_candle(candle_stamp, self.resampled_df)

                self.current_candle_stamp = candle_stamp

                current_stamp = time.time() + 7*3600
                next_candle_time = self._get_next_candle_stamp(current_stamp)
                sleep_duration = next_candle_time - current_stamp

                self._store_resampled_data(self.resampled_df)
                logger.log('INFO', f'Resampled df shape: {self.resampled_df.shape}')

                if sleep_duration > 0:
                    time.sleep(sleep_duration)

                logger.log('INFO', f"Completed processing cycle, next candle at {next_candle_time}")

            except Exception as e:
                logger.log('ERROR', f"Error in realtime resampling: {str(e)}")
                time.sleep(1)

    @classmethod
    def run_resample_history(cls, day, timeframe = '30S', to_plasma=False):
        def test():
            day = '2025_02_18'
            timeframe = '30S'
            to_plasma = True


        df_raw = Adapters.RedisAdapters.load_realtime_stock_data_from_redis(r, day, 0, -1)

        df = df_raw.copy()
        df = ProcessData.preprocess_realtime_stock_data(df_raw, day)


        df = Resampler.transform(day, df, timeframe, cleanup_data=True, subgroups=['stock'])
        df = ProcessData.postprocess_realtime_stock_data(df)

        if to_plasma:
            output_plasma_key = f"pslab_realtime_stockdata.{timeframe}.test"
            Adapters.SaveDataToPlasma.save_data_to_plasma(output_plasma_key, df)

        df = Adapters.load_data_from_plasma("pslab_realtime_stockdata2.30S")


self = Aggregator(
day =  datetime.now().strftime("%Y_%m_%d"),
    timeframe='30S',
    output_plasma_key="pslab_realtime_stockdata2",
    current_pos=200000
)             

if __name__ == "__main__" and not check_run_with_interactive():
    realtime_stock = Aggregator(
        day = datetime.now().strftime("%Y_%m_%d"),
        timeframe='30S',
        output_plasma_key="pslab_realtime_stockdata2"
    )
#%%
    realtime_stock.start_realtime_resampling()