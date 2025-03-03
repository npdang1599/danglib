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

    @classmethod
    def get_agg_dic(self):
        return self.RESAMPLE_AGG_DIC

    @staticmethod
    def ensure_all_stocks_present(df: pd.DataFrame, required_stocks: list, data_name='') -> pd.DataFrame:
        """
        Ensure all stocks are present in DataFrame. Add rows with NaN for missing stocks.
        
        Args:
            df: Original DataFrame
            required_stocks: List of required stock symbols
            
        Returns:
            DataFrame with all required stocks
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
                logger.log('WARNING', f"Missing stocks in data {data_name}: {missing_stocks}")
                
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
                    logger.log('INFO', f"Added {len(new_rows)} rows for missing stocks at {totime(max_timestamp)}")
            
            return df
            
        except Exception as e:
            logger.log('ERROR', f"Error in ensure_all_stocks_present: {str(e)}")
            return df

    class ComputeStats:
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
                df_raw = ProcessStockData.load_realtime_data(r, day, 0, 100000)
                dfi = ProcessStockData.preprocess_realtime_data(df_raw)

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
                        tolerance=15,
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

    @classmethod
    def load_history_data(cls, from_day=None) -> pd.DataFrame:
        def test():
            from_day = '2025_02_18'
            from_stamp = Utils.day_to_timestamp(from_day)

        df: pd.DataFrame = Adapters.load_stock_data_from_plasma()
        
        df = df.drop(['accNetBusd', 'accNetBusd2', 'return'], axis=1, level=0)

        if from_day is not None:
            from_stamp = Utils.day_to_timestamp(from_day)
            df = df[df.index >= from_stamp]
        df.index = df.index // 1e9

        logger.log("INFO",f"Loaded {len(df)} history records from plasma starting from {from_day}")

        return df

    @classmethod
    def load_realtime_data(cls, r, day, start, end) -> pd.DataFrame:
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

            df = cls.ensure_all_stocks_present(df, Globs.STOCKS, data_name="New data")
            return df
        except Exception as e:
            logger.log('ERROR', f"Error in load_realtime_stock_data: {str(e)}")
            return pd.DataFrame()
        
    @classmethod
    def preprocess_realtime_data(cls, df: pd.DataFrame, day) -> pd.DataFrame:
        try:
            if df.empty:
                logger.log('WARNING', "Empty input dataframe in preprocessing")
                return pd.DataFrame()
            df = df.copy()
            df = df[df['stock'].isin(Globs.STOCKS) & (df['close'] != 0)].copy()

            df['timestamp'] = df['timestamp'] // 1000 + 7*60*60
            df['time'] = df['timestamp'] % 86400

            df = df.sort_values(['stock', 'timestamp', 'totalMatchVolume'])

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

            df['open'] = df['high'] = df['low'] = df['close'].copy()

            df['price_diff'] = df.groupby('stock')['close'].diff()
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

            cls.ComputeStats.calculate_bidask(df)

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

            # 1. Group data theo giây trước
            # Group data by stock and time (seconds) first
            df = df.groupby(['stock', 'time'], as_index=False).agg(agg_dic).reset_index()

            # 2. Tính toán foreign_fubon (vẫn có laststate)
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

    @classmethod
    def get_last_states(cls, df:pd.DataFrame, seconds=None, num_dps = None) -> pd.DataFrame:
        """
        Lấy last states của mỗi stock
        """
        if seconds is not None:
            timestamp = df['timestamp'].max()
            fromstamp = timestamp - seconds*1000
            laststates = df[(df['stock'].isin(Globs.STOCKS)) & (df['timestamp'] >= fromstamp)].copy()

            missing_stocks = [s for s in Globs.STOCKS if s not in laststates['stock'].unique()]
            if missing_stocks:
                logger.log('WARNING', f"Missing stocks in last states: {missing_stocks}")
                df_miss = df[df['stock'].isin(missing_stocks)].copy()
                laststates = pd.concat([laststates, df_miss.groupby('stock').tail(1)])

        if num_dps is not None:
            laststates = df.groupby('stock').tail(num_dps)
        return laststates


class ProcessPsData(ProcessData):

    NAME = 'PS'

    SUBGROUP = None
    """
        old_cols = Index(['id', 'time', 'last', 'totalValue', 'day', 'foreignerBuyVolume',
        'foreignerBuyValue', 'foreignerSellVolume', 'foreignerSellValue',
        'totalBidVolume', 'totalAskVolume'],
        dtype='object')
    """

    """
    new_cols = Index(['code', 'best1Bid', 'best1BidVol', 'best2Bid', 'best2BidVol',
    'best3Bid', 'best3BidVol', 'best1Offer', 'best1OfferVol', 'best2Offer',
    'best2OfferVol', 'best3Offer', 'best3OfferVol', 'lastPrice', 'lastVol',
    'highestPrice', 'exchange', 'lowestPrice', 'avgPrice', 'buyForeignQtty',
    'sellForeignQtty', 'priceChange', 'priceChangePercent', 'totalMatchVol',
    'totalMatchValue', 'ceilingPrice', 'floorPrice', 'refPrice', 'session',
    'openPrice', 'closePrice', 'timestamp', 'time', 'symbol', 'matchedBy',
    'best4Bid', 'best4BidVol', 'best5Bid', 'best5BidVol', 'best6Bid',
    'best6BidVol', 'best7Bid', 'best7BidVol', 'best8Bid', 'best8BidVol',
    'best9Bid', 'best9BidVol', 'best10Bid', 'best10BidVol', 'best4Offer',
    'best4OfferVol', 'best5Offer', 'best5OfferVol', 'best6Offer',
    'best6OfferVol', 'best7Offer', 'best7OfferVol', 'best8Offer',
    'best8OfferVol', 'best9Offer', 'best9OfferVol', 'best10Offer',
    'best10OfferVol', 'source']
    """
    REQUIRED_RAW_COLLS = ['id', 'time', 'lastPrice', 'lastVol', 'buyForeignQtty', 'sellForeignQtty', 'totalMatchValue', 'totalMatchVol']
    REQUIRED_OUT_COLLS = [
        'canldleTime', 'F1Open', 'F1High', 'F1Low', 'F1Close', 'F1Volume', 'F1Value', 
        'fF1BuyVol', 'fF1SellVol', 'fPsBuyVol', 'fPsSellVol', 'outstandingFPos']

    REALTIME_F1_COLUMNS_MAPPING = {
        'lastPrice': 'last',
        'totalMatchValue': 'totalValue',
        'totalMatchVol': 'totalVolume',
        'buyForeignQtty':'foreignerBuyVolume',
        'sellForeignQtty':'foreignerSellVolume',
    }

    @classmethod
    def load_history_data(cls, from_stamp) -> pd.DataFrame:
        df: pd.DataFrame = Adapters.load_market_stats_from_plasma()
        df.index = df.index // 1e9

        if from_stamp is not None:
            df = df[df.index >= from_stamp]
        df.index = df.index // 1e9

        """
        df.columns:
        'buyImpact', 'sellImpact', 'Arbit', 'Unwind', 'premiumDiscount',
       'f1Bid', 'f1Ask', 'fF1BuyVol', 'fF1SellVol', 'outstandingFPos',
       'fPsBuyVol', 'fPsSellVol', 'F1Open', 'Vn30Open', 'VnindexOpen',
       'F1High', 'Vn30High', 'VnindexHigh', 'F1Low', 'Vn30Low', 'VnindexLow',
       'F1Close', 'Vn30Close', 'VnindexClose', 'F1Value', 'Vn30Value',
       'VnindexValue', 'F1Volume', 'Vn30Volume', 'VnindexVolume']
        """
        return df[['F1Value', 'F1Volume', 'outstandingFPos', 'fF1BuyVol', 'fF1SellVol', 'fPsBuyVol',
                    'fPsSellVol', 'F1Open', 'F1High', 'F1Low', 'F1Close']]
    
    @classmethod
    def load_realtime_data(cls, r, day, start, end) -> pd.DataFrame:
        def test():
            day = "2025_02_26"
            start = 0
            end = 100000
            r = StrictRedis(decode_responses=True)
            cls = ProcessPsData

        df = Adapters.RedisAdapters.load_realtime_PS_data_from_redis(r, day, start, end)
        df = df[cls.REQUIRED_RAW_COLLS].copy()
        missing_cols = [col for col in cls.REQUIRED_RAW_COLLS if col not in df.columns]
        if missing_cols:
            logger.log('ERROR', f"Missing columns in input {cls.NAME} data: {missing_cols}")
            return pd.DataFrame()

        if df.empty:
            logger.log('WARNING', f"Empty realtime dataframe in {cls.NAME} load_realtime_data")
            return pd.DataFrame()

        try:
            df = df.rename(columns=cls.REALTIME_F1_COLUMNS_MAPPING)
            df['day'] = day
            df['time'] = df['time'].astype(str).str[-6:]
            df['timestamp'] = pd.to_datetime(day + ' ' + df['time'], format='%Y_%m_%d %H%M%S').astype(int) // 1e9
            df['time'] = df['timestamp'] % 86400
            return df
        except Exception as e:
            logger.log('ERROR', f"Error in load_realtime_stock_data: {str(e)}")
            return pd.DataFrame()

    
    @classmethod
    def preprocess_realtime_data(cls, df: pd.DataFrame, day) -> pd.DataFrame:
        def test():
            cls = ProcessPsData
            day = "2025_02_26"
            df = cls.load_realtime_data(r, day, 0, 100000)
        try:
            if df.empty:
                logger.log('WARNING', f"Empty input dataframe in {cls.NAME} preprocessing")
                return pd.DataFrame()
            df = df.copy()
            df.sort_values(['foreignerBuyVolume', 'foreignerSellVolume', 'time','day'], inplace=True)

            
            # process F1

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

            required_cols = ['time', 'timestamp', 'F1Open', 'F1High', 'F1Low', 'F1Close', 'F1Volume', 'F1Value', 
                            'fF1BuyVol', 'fF1SellVol', 'fPsBuyVol', 'fPsSellVol', 'outstandingFPos']
            return dff1[required_cols]

        except Exception as e:
            logger.log('ERROR', f"Error in {cls.NAME} preprocess_realtime_data: {str(e)}")
            return pd.DataFrame()
        
    @classmethod
    def postprocess_resampled_data(cls, df: pd.DataFrame):
        # df[['totalBidVolume', 'totalAskVolume']] = df[['totalBidVolume', 'totalAskVolume']].ffill.fillna(0)
        df[['fF1BuyVol', 'fF1SellVol']] = df[['fF1BuyVol', 'fF1SellVol']].fillna(0)
        return df
    
    @classmethod
    def get_last_states(cls, df:pd.DataFrame, seconds=None, num_dps = None) -> pd.DataFrame:
        """
        Lấy last states của mỗi stock
        """
        if seconds is not None:
            timestamp = df['timestamp'].max()
            fromstamp = timestamp - seconds
            laststates = df[(df['timestamp'] >= fromstamp)].copy()
        if num_dps is not None:
            laststates = df.groupby('id').tail(num_dps)
        return laststates

    @classmethod
    def get_agg_dic(self, stats: list =None):
        if stats is None:
            stats = self.REQUIRED_OUT_COLLS

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
    def transform(cls, day, df: pd.DataFrame, timeframe: str, agg_dic: dict, cleanup_data: bool = False, subgroup=None):
        df = df.copy()
        if cleanup_data:
            df = cls.cleanup_timestamp(day, df)
        
        # Calculate new candle times
        df['candleTime'] = cls._calculate_candletime(df['timestamp'], timeframe)

        groups = ['candleTime']
        if subgroup:
            groups.append(subgroup)

        df = df.groupby(by=groups).agg(agg_dic).reset_index()
        if 'timestamp' in df.columns:
            df = df.drop('timestamp', axis=1)

        if subgroup:
            df = df.pivot(index='candleTime', columns=subgroup)
        else:
            df = df.set_index('candleTime')

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

    def __init__(self, day:str, timeframe:str, output_plasma_key:str, data_processor: ProcessData):
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

    def join_historical_data(self, df:pd.DataFrame, ndays:int=10):
        from_day = Adapters.get_historical_day_by_index(-ndays)
        logger.log('INFO', f'Starting to merge historical data from {from_day}...')

        df_his: pd.DataFrame = self.data_processor.load_history_data(from_day)
        
        df_merge = pd.concat([df_his, df])
        df_merge = df_merge.sort_index(ascending=True)

        logger.log('INFO', f'Successfully merged historical data. After merge shape: {df_merge.shape}')
        return df_merge
    
    def _get_next_candle_stamp(self, current_timestamp):
        """Tính thời điểm bắt đầu của nến tiếp theo"""
        current_candle = self._get_candle_stamp(current_timestamp)
        return current_candle + self.timeframe_seconds
    
    def _get_candle_stamp(self, timestamp):
        """Tính thời điểm bắt đầu của nến hiện tại"""
        return (timestamp // self.timeframe_seconds) * self.timeframe_seconds

    def _add_empty_candle(self, candlestamp, df: pd.DataFrame):
        """Tạo nến trống với giá trị NaN cho dữ liệu"""
        # Tạo một DataFrame mới với index là candlestamp và các cột giống df
        new_df = pd.DataFrame(
            index=[candlestamp],
            columns=df.columns,
            data=np.nan
        )
        
        # Concat với DataFrame cũ
        return pd.concat([df, new_df])
    
    def _update_last_states(self, df: pd.DataFrame, seconds=30):
        """Cập nhật last states"""
        # Lấy last states từ DataFrame mới
        last_states = self.data_processor.get_last_states(df, seconds=seconds)
        self.last_states = last_states

    def _append_last_states(self, df: pd.DataFrame) -> pd.DataFrame:
        """Thêm last states vào đầu DataFrame mới để tính diff chính xác"""
        if len(self.last_states) !=0:
            return pd.concat([self.last_states, df])
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
        old_df.columns.names = ['stats', 'stock']
        new_df.columns.names = ['stats', 'stock']
        if not common_idx.empty:
            # Gộp data cho các candles trùng nhau
            overlap_data = pd.concat([
                old_df.loc[common_idx],
                new_df.loc[common_idx]
            ])

            recalculated = overlap_data.stack()
            recalculated = recalculated.groupby(level=[0, 1]).agg(agg_dict).pivot_table(index='candleTime', columns='stock')

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

                raw_data: pd.DataFrame = self.data_processor.load_realtime_data(self.r, self.day, self.current_pos, -1)
                logger.log('INFO', f"Processing data from position {self.current_pos}")
                self.current_pos += len(raw_data) + 1
                logger.log('INFO', f"Updated current_pos to {self.current_pos}")

                if not raw_data.empty:
                    raw_data_with_last_states = self._append_last_states(raw_data)

                    new_data: pd.DataFrame = self.data_processor.preprocess_realtime_data(raw_data_with_last_states, self.day)
                    new_data = new_data[new_data['timestamp'] >= self.current_candle_stamp]
                    print(f"New data num stocks:  {len(new_data['stock'].unique())}")

                    # Cập nhật last states cho lần sau
                    self._update_last_states(raw_data_with_last_states, seconds=30)

                    if not new_data.empty:
                        # Resample dữ liệu đã preprocess
                        new_resampled = Resampler.transform(
                            day = self.day,
                            df = new_data,
                            timeframe=self.timeframe,
                            agg_dic=self.data_processor.get_agg_dic(),
                            cleanup_data=True,
                            subgroup = self.data_processor.SUBGROUP
                        )
                        print(f"New resampled data shape: {new_resampled.info()}")

                        if self.resampled_df.empty:
                            self.resampled_df = self.join_historical_data(new_resampled, ndays=1)
                            print(f"Init resampled df shape: {self.resampled_df.info()}")
                        else:
                            self.resampled_df = self.merge_resampled_data(
                                old_df = self.resampled_df, 
                                new_df=new_resampled,
                                agg_dict = self.data_processor.get_agg_dic()
                            )

                            print(f"Updated resampled df shape: {self.resampled_df.info()}")
                        
                        self.resampled_df = self.data_processor.postprocess_resampled_data(self.resampled_df)

                        current_candle_stamp = self._get_candle_stamp(new_data['timestamp'].max())
                        if current_candle_stamp > self.current_candle_stamp:
                            self.current_candle_stamp = current_candle_stamp
                        else:
                            self.current_candle_stamp = self._get_next_candle_stamp(self.current_candle_stamp)
                            self._add_empty_candle(self.current_candle_stamp, self.resampled_df)
                            logger.log('INFO', f"Added empty candle at {self.current_candle_stamp}")

                        logger.log('INFO', f"Updated current_candle_stamp to {self.current_candle_stamp}")
                        
                        self._store_resampled_data(self.resampled_df)
                        logger.log('INFO', f"Stored resampled data to Plasma key: {self.output_key}")

                current_stamp = time.time() + 7*3600
                next_candle_time = self._get_next_candle_stamp(current_stamp)
                sleep_duration = next_candle_time - current_stamp

                logger.log('INFO', f"Completed processing cycle, next candle at {next_candle_time}")
                
                if sleep_duration > 0:
                    time.sleep(sleep_duration)

                

            except Exception as e:
                logger.log('ERROR', f"Error in realtime resampling: {str(e)}", exc_info=True)
                time.sleep(1)

class args:
    timeframe = '30S'
    type = 'stock'

current_day = datetime.now().strftime("%Y_%m_%d")
output_key = 'pslab_realtime_psdata2'
processor = ProcessStockData

self = Aggregator(
    day=current_day,
    timeframe='30S',
    output_plasma_key='pslab_realtime_psdata2',
    data_processor=processor
)

import argparse


if __name__ == "__main__" and not check_run_with_interactive():
    # Create argument parser
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
    
    
    # Configure processor based on type argument
    if args.type == 'stock':
        processor = ProcessStockData
        output_key = 'pslab_realtime_stockdata2'
    else:  # ps
        processor = ProcessPsData
        output_key = 'pslab_realtime_psdata2'
    
    # Create and start aggregator
    aggregator = Aggregator(
        day=current_day,
        timeframe=args.timeframe,
        output_plasma_key=output_key,
        data_processor=processor
    )
    
    aggregator.start_realtime_resampling()