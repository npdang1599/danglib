import warnings
import logging
import json

import pandas as pd
import numpy as np
import pickle, zlib
import pyarrow.parquet as pq
from redis import StrictRedis

from functools import reduce
from pymongo import MongoClient
from datetime import datetime

from danglib.Adapters.pickle_adapter_ssi_ws import PICKLED_WS_ADAPTERS
from danglib.Adapters.adapters import MongoDBs
from danglib.pslab.utils import day_to_timestamp, FileHandler, unflatten_columns, RedisHandler

r = StrictRedis()

pd.options.mode.chained_assignment = None
# Turn off all FutureWarnings
warnings.simplefilter(action='ignore', category=FutureWarning)

class Globs:
    STOCKS = ['VIX', 'CTS', 'ORS', 'FTS', 'AGR', 'GEX', 'BSI', 'VCI', 'DIG', 'VND', 'VDS', 'DXG', 'DGW', 'PDR', 'HCM', 'CII', 'HTN', 'GVR', 'NKG', 'HSG', 'TCI', 'NVL', 'SSI', 'GIL', 'KSB', 'NLG', 'KBC', 'FCN', 'LCG', 'DPG', 'DBC', 'TCH', 'VOS', 'VPG', 'HDC', 'ANV', 'VCG', 'PET', 'VGC', 'PC1', 'HAH', 'ASM', 'IJC', 'BCG', 'HHV', 'DXS', 'CSV', 'IDI', 'SZC', 'HHS', 'CTD', 'KHG', 'ADS', 'TLH', 'DCM', 'DGC', 'SCR', 'MWG', 'PVD', 'AAA', 'PVT', 'MSN', 'CMX', 'VTP', 'LSS', 'TIP', 'DPM', 'HDG', 'VSC', 'HQC', 'HPG', 'SMC', 'EVF', 'NTL', 'PAN', 'VHC', 'CNG', 'VRE', 'EIB', 'STB', 'TCB', 'KDH', 'MSB', 'VHM', 'TV2', 'CTR', 'LDG', 'CTI', 'SHB', 'MSH', 'CTG', 'ELC', 'PHR', 'VIB', 'VIC', 'PSH', 'TTF', 'BFC', 'TPB', 'LHG', 'SIP', 'OCB', 'SKG', 'DPR', 'GMD', 'VPB', 'MBB', 'BID', 'APH', 'HAG', 'PLX', 'AGG', 'HAX', 'SBT', 'LPB', 'BMP', 'BCM', 'GEG', 'FPT', 'POW', 'HVN', 'PVP', 'TNH', 'DHC', 'NT2', 'DRC', 'BVH', 'REE', 'FRT', 'ACB', 'CMG', 'GAS', 'GSP', 'VIP', 'HDB', 'DHA', 'SAB', 'VTO', 'PNJ', 'BAF', 'NAF', 'YEG', 'VJC', 'VNM', 'NAB', 'PTB', 'VCB', 'ITD', 'TCM', 'VPI', 'SJS', 'SSB', 'SCS', 'BWE', 'NCT', 'KDC']
    STOCKS_S2I = {s: i for i, s in enumerate(STOCKS)}
    STOCKS_I2S = {i: s for i, s in enumerate(STOCKS)}
    DATA_FROM_DAY = '2022_05_30'
    DATA_FROM_TIMESTAMP = day_to_timestamp(DATA_FROM_DAY)
    
    BASE_TIMEFRAME = '30S'
    
    MAKETSTATS_SRC = ['buyImpact', 'sellImpact', 'Arbit', 'Unwind', 'premiumDiscount',
                    'f1Bid', 'f1Ask', 'fF1BuyVol', 'fF1SellVol', 'outstandingFPos',
                    'fPsBuyVol', 'fPsSellVol', 'F1Open', 'Vn30Open', 'VnindexOpen',
                    'F1High', 'Vn30High', 'VnindexHigh', 'F1Low', 'Vn30Low', 'VnindexLow',
                    'F1Close', 'Vn30Close', 'VnindexClose', 'F1Value', 'Vn30Value',
                    'VnindexValue', 'F1Volume', 'Vn30Volume', 'VnindexVolume']
    
    STOCKSTATS_SRC = ['open', 'high', 'low', 'close', 'value', 'bu', 'sd', 'bu2', 'sd2', 'bid', 'ask', 'refPrice', 'fBuyVal', 'fSellVal', 'return', 'accNetBusd', 'accNetBusd2']
    
    DAILYINDEX_SRC = ['F1Open', 'Vn30Open', 'VnindexOpen', 'F1High', 'Vn30High', 'VnindexHigh', 'F1Low', 'Vn30Low', 'VnindexLow', 'F1Close', 'Vn30Close', 'VnindexClose', 'F1Value', 'Vn30Value', 'VnindexValue']
    
    STOCKS_SRC = ['bu', 'sd', 'bu2', 'sd2', 'bid', 'ask', 'fBuyVal', 'fSellVal', 'value']

    GROUP_SRC = ['ask', 'bid', 'bu', 'bu2', 'fBuyVal', 'fSellVal','sd', 'sd2', 'value']

    STANDARD_AGG_DIC = {
        'buyImpact': 'last',
        'sellImpact': 'last',
        'Arbit': 'sum',
        'Unwind': 'sum',
        'premiumDiscount': 'last',
        'f1Bid': 'last',
        'f1Ask': 'last',
        'Vn30Value': 'sum',
        'Vn30Volume': 'sum',
        'F1Value': 'sum',
        'F1Volume': 'sum',
        'VnindexValue': 'sum',
        'VnindexVolume': 'sum',
        'f1BuyVol': 'sum',
        'f1SellVol': 'sum',
        'outstandingFPos': 'last',
        'fPsBuyVol': 'sum', 
        'fPsSellVol': 'sum',
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
        'fBuyVal': 'sum',
        'fSellVal': 'sum',
        'return': 'last',
        'F1Open': 'first',
        'Vn30Open': 'first',
        'VnindexOpen': 'first',
        'F1High': 'max',
        'Vn30High': 'max',
        'VnindexHigh': 'max',
        'F1Low': 'min', 
        'Vn30Low': 'min',
        'VnindexLow': 'min',
        'F1Close': 'last',
        'Vn30Close': 'last',
        'VnindexClose': 'last',
        'day': 'last',
        'timestamp': 'last',
        'foreignerBuyVolume': 'last',
        'foreignerSellVolume': 'last',
        'volume': 'sum',
        'value': 'sum',
        'fF1BuyVol': 'sum',
        'fF1SellVol': 'sum',
    }


    SECTOR_DIC = {
        'Super High Beta': ['SHS', 'VGS', 'MBS', 'VIX', 'CTS', 'ORS', 'CEO', 'FTS', 'DTD', 'AGR', 'GEX', 'BSI', 'HUT', 'VCI', 'DIG', 'VND', 'VDS', 'L14', 'DXG', 'DGW', 'PDR', 'HCM', 'CII', 'HTN', 'GVR', 'NKG', 'BVS', 'HSG', 'TCI', 'NVL', 'SSI', 'GIL', 'PXL', 'KSB', 'PLC', 'NLG', 'KBC', 'DDV', 'FCN', 'LCG', 'DPG', 'DBC', 'TCH', 'VOS', 'VPG', 'HDC', 'IDJ', 'ANV', 'VCG', 'PET', 'VGC', 'PC1', 'HAH', 'ASM'], 
        'High Beta': ['IJC', 'C4G', 'BCG', 'HHV', 'DXS', 'CSV', 'IDI', 'TNG', 'SZC', 'HHS', 'CTD', 'KHG', 'ADS', 'PVC', 'TLH', 'DCM', 'DGC', 'SCR', 'S99', 'TIG', 'MWG', 'LAS', 'PVD', 'AAA', 'PVT', 'POM', 'MSN', 'HBC', 'PVS', 'CMX', 'VTP', 'PVB', 'LSS', 'IDC', 'TIP', 'DPM', 'HDG', 'VSC', 'HQC', 'HPG', 'SMC', 'EVF', 'NTL', 'VGI', 'DRI', 'PAN', 'VHC', 'CSC', 'CNG'], 
        'Medium': ['VRE', 'EIB', 'STB', 'DSC', 'TCB', 'KDH', 'MSB', 'VHM', 'TV2', 'CTR', 'LDG', 'VGT', 'CTI', 'BSR', 'SHB', 'MSH', 'CTG', 'ELC', 'PHR', 'VIB', 'VIC', 'PSH', 'TTF', 'BFC', 'TPB', 'MSR', 'LHG', 'VCS', 'SIP', 'OCB', 'SKG', 'DPR', 'GMD', 'VPB', 'MBB', 'BID', 'APH'], 
        'Low': ['HAG', 'PLX', 'AGG', 'HAX', 'SBT', 'LPB', 'BMP', 'BCM', 'GEG', 'FPT', 'POW', 'HVN', 'PVP', 'TNH', 'DHC', 'NT2', 'OIL', 'DRC', 'BVH', 'REE', 'FRT', 'ABB', 'ACB', 'HNG', 'CMG', 'GAS', 'GSP', 'VIP', 'HDB', 'DHA', 'SAB', 'VTO', 'PNJ', 'BAF', 'QNS', 'NAF', 'YEG', 'VJC', 'VNM', 'NAB', 'PTB', 'VCB', 'ITD', 'TCM', 'VPI', 'VEA', 'SJS', 'MCH', 'SSB', 'FOX', 'ACV', 'SCS', 'BWE', 'NCT', 'KDC'],
        "VN30": ['ACB', 'BCM', 'BID', 'BVH', 'CTG', 'FPT', 'GAS', 'GVR', 'HDB', 'HPG', 'MBB', 'MSN', 'MWG', 'PLX', 'POW', 'SAB', 'SHB', 'SSB', 'SSI', 'STB', 'TCB', 'TPB', 'VCB', 'VHM', 'VIB', 'VIC', 'VJC', 'VNM', 'VPB', 'VRE'],
        "All": STOCKS,
    }

    TF_TO_MIN = {
        '30S' : 0.5, 
        '1Min' : 1, 
        '5Min' : 5, 
        '10Min' : 10,
        '15Min' : 15,
        '30Min' : 30,
        '45Min' : 45,
        '1H' : 60,
        '2H' : 120,
        '1D' : 1440,
    }
    ROLLING_TIMEFRAME = list(TF_TO_MIN.keys())


    ROLLING_METHOD = ['sum', 'median', 'mean', 'rank']

    USE_SAMPLE_DATA = False

    PLASMA_DB = 10

    class RedisPatterns:
        PSLAB_STOCKCOUNT = 'pslab/stockcount/*'

    VN30_WEIGHT = None
    
    @classmethod
    def load_vn30_weight(cls):
        cls.VN30_WEIGHT = Adapters.load_vn30_weights_from_db()
    
    @staticmethod
    def get_sample_from_day():
        day = Adapters.get_historical_day_by_index(-5)
        stamp = day_to_timestamp(day)
        return stamp



class Resources:
    CACHED_FOLDER = "/data/dang"
    FILES_FOLDER = "/home/ubuntu/Dang/pslab/files"
    LOGS_FOLDER = "/home/ubuntu/Dang/pslab/logs"
    SAMPLE_DATA_FOLDER = '/home/ubuntu/Dang/pslab/sample_data'

    @staticmethod
    def get_cached_store_dir(timeframe):
        store_dir = f"{Resources.CACHED_FOLDER}/hose156_{timeframe}"
        return store_dir

    @staticmethod
    def get_cached_store_dir_index(timeframe):
        store_dir = f"{Resources.CACHED_FOLDER}/index_{timeframe}"
        return store_dir

    @staticmethod
    def get_aggregated_data(timeframe, extension='.pickle'):
        return f"{Resources.CACHED_FOLDER}/hose156_{timeframe}{extension}"

    @staticmethod
    def get_group_stats_aggregated(timeframe, group=None, extension='.pickle'):
        if group is None:
            group = "All"
        return f"{Resources.CACHED_FOLDER}/hose156_group_{timeframe}_{group}{extension}"
    
    @staticmethod
    def get_market_stats_aggregated(timeframe, extension='.pickle'):
        return f"{Resources.CACHED_FOLDER}/hose156_market_{timeframe}{extension}"

    @staticmethod
    def get_index_ohlc_aggregated(timeframe, extension='.pickle'):
        return f"{Resources.CACHED_FOLDER}/index_ohlc_{timeframe}{extension}"

    @staticmethod
    def get_buy_sell_impact_fn(timeframe, extension='.pickle'):
        return f"{Resources.CACHED_FOLDER}/buy_sell_impact_{timeframe}{extension}"

    @staticmethod
    def get_arbit_unwind_aggregated(timeframe, extension='.pickle'):
        return f"{Resources.CACHED_FOLDER}/arbit_unwind_{timeframe}{extension}"
    
    @staticmethod
    def get_f1_bidask_fn(timeframe, extension='.pickle'):
        return f"{Resources.CACHED_FOLDER}/f1_bidask_{timeframe}{extension}"
    
    class PlasmaKeys:
        STOCK_DATA = "pslab_stock_data"
        MARKET_DATA = "pslab_market_data"
        INDEX_OHLC = "pslab_ohlcv_index_data"
        GROUP_STATS = "pslab_group_stats_data"
        INDEX_DAILY_OHLC = "pslab_daily_index_ohlcv"
        STOCK_CLASSIFICATION = "pslab_stock_classification"
        STOCK_DATA_REALTIME = "pslab_realtime_stockdata.30S"    

    class RedisKeys:
        @staticmethod
        def get_hose500_realtime_key(day):
            return f"pylabview_hose_{day}"
        
        @staticmethod
        def get_PS_realtime_key(day):
            return f"pylabview_ps_{day}"
        
        @staticmethod
        def get_index_realtime_key(day):
            return f"pylabview_index_{day}"
        

# Add logging configuration near the top after imports
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'{Resources.LOGS_FOLDER}/update_cache_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)


class Adapters:
    def load_arbit_unwind_data(day):
        db = MongoDBs.cmc_curated()
        data_dict = list(db.get_collection(f"{day}_arbit").find({},{'_id':0}))
        df = pd.DataFrame(data_dict)
        return df

    def load_vn30_weights_from_db(day=None):
        db = MongoClient('ws', 27021)['weights']
        coll = db['vn30_weights']
        if day is None:
            day = datetime.now().strftime("%Y_%m_%d")
        df = list(coll.find({'day':{'$lte': day}},{'_id':0}).sort('day', -1).limit(30))
        if len(df) == 0:
            df = list(coll.find({'day':{'$gte': day}},{'_id':0}).sort('day', 1).limit(30))
        df = pd.DataFrame(df)
        return df

    @staticmethod
    def load_stock_data_from_plasma_realtime(required_stats: list = None, stocks = None) -> pd.DataFrame:
        def test(): 
            required_stats = ['bu', 'sd']
            groups_and_stocks = ['HPG', 'SSI']
            stocks = ['All']

        df = Adapters.load_data_from_plasma(key=f"pslab_realtime_stockdata2.30S", db=11)

        if required_stats is not None:
            df = df[required_stats]

        true_stocks = []
        if stocks is not None:
            for s in stocks:
                if s in Globs.SECTOR_DIC.keys():
                    true_stocks += Globs.SECTOR_DIC[s]
                elif s in Globs.STOCKS:
                    true_stocks.append(s)

        true_stocks = [i for i in true_stocks if i in Globs.STOCKS]

        if len(true_stocks) > 0:
            df = df.loc[:, (slice(None), true_stocks)]

        return df
    
    @staticmethod
    def load_market_stats_from_plasma_realtime(required_stats: list = None):
        def test(): 
            required_stats = ['buyImpact', 'sellImpact']

        df = Adapters.load_data_from_plasma(key=f"pslab_market_stats_realtime", db=11)

        if required_stats is not None:
            df = df[required_stats]

        return df


    @staticmethod
    def load_data_from_plasma(key, db=None):
        try:
            if db is None:
                db = Globs.PLASMA_DB

            from danglib.lazy_core import gen_plasma_functions
            _, disconnect, psave, pload = gen_plasma_functions(db)

            df: pd.DataFrame = pload(key)

            return df

        finally:
            disconnect()
    
    
    @staticmethod
    def save_data_to_plasma(key, data, db=None):
        try:
            if db is None:
                db = Globs.PLASMA_DB
            from danglib.lazy_core import gen_plasma_functions
            _, disconnect, psave, pload = gen_plasma_functions(db)

            psave(key, data)

        finally:
            disconnect()


    @staticmethod
    def load_put_through_data_realtime(r: StrictRedis, day):
        key = f"redis_tree.thoathuan.df_tt.{day}"
        df = pd.DataFrame(json.loads(r.get(key)))
        return df

    @staticmethod
    def load_thoathuan_dc_data_from_db(day):
        client = MongoClient('cmc', 27017)
        db = client['dc_data']
        coll_name = f"thoa_thuan_2"
        data = list(db[coll_name].find({'day':day}, {"_id": 0}))
        if not data or 'data' not in data[0] or 'putExecs' not in data[0]['data']:
            logging.warning(f"No data found or invalid structure for day: {day}")
            df = pd.DataFrame(columns=['stock', 'time_str', 'vol'])
        else:
            df = pd.DataFrame(data[0]['data']['putExecs'])
        return df

    def load_ssi_VN30_data_from_db(day):
        db = MongoDBs.cmc_curated()
        coll_name = f'{day}_KIS'

        data = list(db[coll_name].find({}, {"_id": 0}))
        list_df = [pd.DataFrame(data[i]) for i in range(len(data))]
        df = pd.concat(list_df,ignore_index=True)

        if len(df) > 0:
            if 'timestamp' not in df.columns:
                df['timestamp'] = pd.to_datetime(day + ' ' + df['time'], format="%Y_%m_%d %H%M%S")
                df['timestamp'] = df['timestamp'].view(int) // 10**6 + 7*60*60*1000

            df = df[
                    ['timestamp', 'code', 'last','matchingVolume', 'matchedBy']
                ][df['matchingVolume'] > 0]
        else:
            df = pd.DataFrame()

        return df

    @staticmethod   
    def get_data_days(custom_days = None, run_custom_days = False):

        if run_custom_days:
            return custom_days

        db = PICKLED_WS_ADAPTERS.get_target_pickled_db()
        drop_day = ['2022_01_31', '2022_02_01', '2022_02_02', '2022_02_03', '2022_02_04', '2022_04_11', '2022_05_02', '2022_05_03', '2022_09_01', '2022_09_02', '2023_01_02']
        return [d for d in sorted(db.list_collection_names()) if d >= Globs.DATA_FROM_DAY and d not in drop_day]

    @staticmethod
    def get_max_datatime():
        max_col = max(Adapters.get_data_days())
        return max_col
    
    @staticmethod
    def load_data_only(day=None):
        if day is None:
            day = '2025_03_28'

        db = PICKLED_WS_ADAPTERS.load_hose500_from_db(day)
        df = pd.DataFrame.from_dict(db.get("data"))

        return df
    
    @staticmethod
    def load_data_and_preprocess(day=None):
        def test():
            day = "2024_10_25"
        if day is None:
            day = Adapters.get_max_datatime()

        db = PICKLED_WS_ADAPTERS.load_hose500_from_db(day)
        df = pd.DataFrame.from_dict(db.get("data"))
        df['time'] = np.where(df['time'].astype(int) >= 10144500, 10144500, df['time'].astype(int))
        df['datetime'] = day + ' ' + df['time'].astype(str).str[2:]

        return df[df['stock'].isin(Globs.STOCKS) & (df['last'] != 0)].copy()
    
    @staticmethod
    def load_refPrice_data(day):
        client = MongoClient('ws',27022)
        db = client['stockdata']
        coll = db['price_fiinpro_data']
        
        df_refPrice = pd.DataFrame(coll.find({'TradingDate':day.replace('_','-')},{'_id':0,"Ticker":1, "ReferencePrice":1}))
        return df_refPrice
    
    @staticmethod
    def get_adjusted_ratio_db():
        client = MongoClient('ws',27022)
        db = client['stockdata']
        return db["fiinpro_adjusted_ratio"]
    
    @staticmethod
    def load_aggregated_resampled_data(timeframe, stocks=None, columns=None, from_day=None):
        def test():
            timeframe = '30S'
            stocks = ['HPG', 'SSI', 'TCB', 'MSN', 'PDR', 'VCI', 'VIX']

        if from_day is None:
            from_day = Globs.DATA_FROM_DAY
        
        from_timestamp = day_to_timestamp(from_day)

        resampled_data, stocks_mapping= pd.read_pickle(Resources.get_aggregated_data(timeframe))
        resampled_data: pd.DataFrame

        df = resampled_data
        if stocks is not None:
            i_stocks = [i for i in stocks_mapping.keys() if stocks_mapping[i] in stocks]
            df = df[df['stock'].isin(i_stocks)].copy()
        
        df['stock'] = df['stock'].map(stocks_mapping)
        
        if columns is not None:
            df = df[columns].copy()

        return df[df['candleTime'] >= from_timestamp]
    
    @staticmethod
    def load_VN30_VNINDEX_tickdata(day):
        db = MongoClient(host='ws', port=27022)['ssi_ws_index']
        name = f"{day}_index"
        col = db[name]
        df_zip = pd.DataFrame(col.find().sort('_id', 1))
        zipped_binary_data = reduce(
            lambda a, b: a + b,
            df_zip['pickledData'])
        df = pickle.loads(zlib.decompress(zipped_binary_data))
        df = pd.DataFrame(df)
        df = df[df['id'].isin(['VN30', 'VNINDEX'])]
        df = df[['id', 'time', 'last', 'totalValue']].copy()
        df['day'] = day
        return df
    
    @staticmethod
    def load_f1_tickdata(type="F1", day="2022_10_10"):
        """
        input: 
        - type <str> : F1, F2 or F3
        - day <str> : date in 2022
        output:
        - pd.DataFrame : tick data of F1, F2 or F3
        """
        def test():
            type="F1"
            day = "2024_11_26"
        

        cli = MongoClient(
            host='ws', 
            port=27022,
            connectTimeoutMS=2000,
            serverSelectionTimeoutMS=2000)
        db = cli['ssi_ps_data']
        col = db[f'{day}_{type.lower()}']   
        data = list(
            col.find(
                {},
                {
                "_id": 0,
                "time": 1,
                "last": 1,
                "totalMatchVolume":1,
                "totalAskVolume":1,
                "totalBidVolume":1
                }
            )
        )

        df = pd.DataFrame(data)
        df['day'] = day
        df['time'] = "10" + df['time'].str.replace(":", "")
        df['id'] = "F1"
        df = df.rename(columns={'totalMatchVolume': 'totalValue'})

        return df
    
    @staticmethod
    def load_VN30_VNINDEX_F1_tickdata(day):
        def test():
            day = '2024_10_21'
        df_idx = Adapters.load_VN30_VNINDEX_tickdata(day)
        df_f1 = Adapters.load_f1_tickdata(day=day)

        res = pd.concat([df_idx, df_f1]) 

        return res
    
    sector_dic = Globs.SECTOR_DIC

    @staticmethod
    def get_stocks_group(group_name="VN30"):
        return Adapters.sector_dic.get(group_name, None)

    @staticmethod
    def load_stock_data_from_parquet(required_stats: list = None, stocks: list = None, data_path: str = None) -> pd.DataFrame:
        """Load only required columns from parquet file

        Args:
            required_cols: List of column names needed
            data_path: Path to parquet file
            
        Returns:
            pd.Dataframe
        """
        def test():
            required_stats = ['bu', 'sd']
            stocks = ['HPG', 'SSI', 'VPB']
        
        if required_stats is None:
            required_stats = Globs.STOCKSTATS_SRC

        if stocks is not None:
            required_cols = [f"{x}_{y}" for x in required_stats for y in stocks]
        else:
            required_cols = required_stats

        if data_path is None:
            data_path = Resources.get_aggregated_data('30S', '.parquet')

        columns = FileHandler.get_parquet_columns(data_path, required_cols)

        df = pd.read_parquet(data_path, columns=columns)
        df = unflatten_columns(df)
        
        return df
    
    @staticmethod
    def load_market_stats_from_parquet(required_cols: list=None) -> pd.DataFrame:
        """Load only required columns from parquet file

        Args:
            required_cols: List of column names needed
            
        Returns:
            pd.DataFrame
        """
        data_path = Resources.get_market_stats_aggregated('30S', '.parquet')
        
        df = pd.read_parquet(data_path, columns=required_cols)
        return df
    
    @staticmethod
    def load_index_ohlc_from_pickle(name: str = None, timeframe: str='30S') -> pd.DataFrame:
        """Load index OHLC data from pickle file."""
        df: pd.DataFrame = pd.read_pickle(Resources.get_index_ohlc_aggregated(timeframe))
        days = df.index.astype(str).str[:10].str.replace('-','_') 
        df.index = df.index.astype(int)
        

        if name is not None:
            df = df[[c for c in df.columns if name in c]].copy()
            df.columns = [c.split('_')[1] for c in df.columns]
        
        df['day'] = days
        return df.sort_index()
    
    @staticmethod
    def load_f1_daily_from_db():

        db = MongoClient(port=27022)['stockdata']
        col = db['price_ps_data']

        df1 = pd.DataFrame(list(col.find({},{"_id":0})))
        df1['timestamp'] = 'F1'
        df1 = df1.rename(columns={'timestamp': 'stock', 'volumn': 'volume'})
        df1 = df1.sort_values('day')
        return df1
    
    class SaveDataToPlasma:
        @staticmethod
        def save_data_to_plasma(key, data, db=None):
            try:
                if db is None:
                    db = Globs.PLASMA_DB
                from danglib.lazy_core import gen_plasma_functions
                _, disconnect, psave, pload = gen_plasma_functions(db)

                psave(key, data)
            except Exception as e:
                logging.error(f"Error saving data to plasma: {str(e)}")
            finally:
                disconnect()

        @staticmethod
        def save_stock_data_to_plasma(create_sample=False):
        
            from danglib.lazy_core import gen_plasma_functions
            _, disconnect, psave, pload = gen_plasma_functions(Globs.PLASMA_DB)

            key = Resources.PlasmaKeys.STOCK_DATA

            def test():
                df, smap = pd.read_pickle(Resources.get_aggregated_data('30S'))


            df = Adapters.load_stock_data_from_parquet()

            if create_sample:
                dfs = df[df.index > Globs.get_sample_from_day()]
                FileHandler.write_parquet(f"{Resources.SAMPLE_DATA_FOLDER}/{key}.parquet", dfs)

            psave(key, df)

            disconnect()

        @staticmethod
        def save_market_data_to_plasma(create_sample=False):
            from danglib.lazy_core import gen_plasma_functions
            _, disconnect, psave, pload = gen_plasma_functions(Globs.PLASMA_DB)
            key = Resources.PlasmaKeys.MARKET_DATA

            df_market_stats = Adapters.load_market_stats_from_parquet()
            df_market_stats['F1Volume'] = df_market_stats['F1Value'].copy()
            df_market_stats['F1Value'] = df_market_stats['F1Value'] * df_market_stats['F1Close'] * 100000
            df_market_stats['Vn30Volume'] = df_market_stats['Vn30Value'].copy()
            df_market_stats['VnindexVolume'] = df_market_stats['VnindexValue'].copy()

            df_market_stats = df_market_stats.rename(columns={
                'f1BuyVol': "fF1BuyVol",
                'f1SellVol': "fF1SellVol",
                'fBuyVol_ps': "fPsBuyVol",
                'fSellVol_ps': "fPsSellVol"
            })

            if create_sample:
                dfs = df_market_stats[df_market_stats.index > Globs.get_sample_from_day()]
                FileHandler.write_parquet(f"{Resources.SAMPLE_DATA_FOLDER}/{key}.parquet", dfs)

            psave(key, df_market_stats)

            disconnect()

        @staticmethod
        def save_index_ohlcv_to_plasma(create_sample=False):

            from danglib.lazy_core import gen_plasma_functions
            _, disconnect, psave, pload = gen_plasma_functions(Globs.PLASMA_DB)

            key = Resources.PlasmaKeys.INDEX_OHLC

            df = Adapters.load_index_ohlc_from_pickle()
            df['F1_volume'] = df['F1_value'].copy()
            df['F1_value'] = df['F1_value'] * df['F1_close'] * 100000
            df['VN30_volume'] = df['VN30_value'].copy()
            df['VNINDEX_volume'] = df['VNINDEX_value'].copy()

            if create_sample:
                dfs = df[df.index > Globs.get_sample_from_day()]
                FileHandler.write_parquet(f"{Resources.SAMPLE_DATA_FOLDER}/{key}.parquet", dfs)

            psave(f"{key}", df)

            disconnect()
        
        @staticmethod
        def save_group_data_to_plasma(create_sample=False):
            from danglib.lazy_core import gen_plasma_functions
            _, disconnect, psave, pload = gen_plasma_functions(Globs.PLASMA_DB)

            key = Resources.PlasmaKeys.GROUP_STATS

            df = Adapters.load_stock_data_from_plasma()
            df = df.groupby(level=0, axis=1).sum()

            # ls = []
            # for g in Globs.SECTOR_DIC.keys():
            #     path = Resources.get_group_stats_aggregated('30S', group=g)
            #     df_tmp: pd.DataFrame = pd.read_pickle(path)
            #     df_tmp = df_tmp.rename(columns={c:f"{c}_{g}" for c in df_tmp.columns})

            #     ls.append(df_tmp)
            
            # df = pd.concat(ls, axis=1).sort_index()

            # df = unflatten_columns(df)
            # df = df.sort_index(axis=1)
            # df = df.rename(columns={'fBuyVol': 'fBuyVal', 'fSellVol': 'fSellVal', 'matchingValue': 'value'})

            if create_sample:
                dfs = df[df.index > Globs.get_sample_from_day()]
                FileHandler.write_parquet(f"{Resources.SAMPLE_DATA_FOLDER}/{key}.parquet", dfs)

            psave(f"{key}", df)

            disconnect()

        @staticmethod
        def save_index_daily_ohlcv_to_plasma(create_sample: bool = False):

            from danglib.lazy_core import gen_plasma_functions
            _, disconnect, psave, pload = gen_plasma_functions(Globs.PLASMA_DB)

            key = Resources.PlasmaKeys.INDEX_DAILY_OHLC

            from danglib.utils import flatten_columns, underscore_to_camel

            df_f1: pd.DataFrame = Adapters.load_f1_daily_from_db()

            cols = ['open', 'high', 'low', 'close', 'volume']
            df = Adapters.load_daily_stock_data_from_plasma(['VNINDEX', 'VN30'])
            df = pd.concat([df, df_f1])
            df = df[['stock', 'day'] + cols].copy()
            df = df[df['day'] >= Globs.DATA_FROM_DAY]
            df_pivot = df.pivot(index='day', columns='stock', values=cols).swaplevel(axis=1)

            dfr = flatten_columns(df_pivot)
            dfr = dfr.rename(columns= {k: underscore_to_camel(k) for k in dfr.columns})

            dfr = dfr.rename(columns={'VnindexVolume': 'VnindexValue', 'Vn30Volume': 'Vn30Value'})
            dfr['F1Value'] = dfr['F1Volume'] * dfr['F1Close'] * 100000
            dfr['Vn30Volume'] = dfr['Vn30Value'].copy()
            dfr['VnindexVolume'] = dfr['VnindexValue'].copy()

            if create_sample:
                FileHandler.write_parquet(f"{Resources.SAMPLE_DATA_FOLDER}/{key}.parquet", dfr)

            psave(key, dfr)

            disconnect()

        @staticmethod
        def run_save_all(save_sample_data_to_plasma: bool = False):
            if save_sample_data_to_plasma:
                group_data = Adapters.load_group_data_from_plasma(load_sample=True)
                stock_data = Adapters.load_stock_data_from_plasma(load_sample=True)
                market_data = Adapters.load_market_stats_from_plasma(load_sample=True)
                index_data = Adapters.load_index_ohlcv_from_plasma(load_sample=True)
                index_daily_data = Adapters.load_index_daily_ohlcv_from_plasma(load_sample=True)
                stocks_classified = Adapters.load_stock_classification_from_plasma(load_sample=True)

                from danglib.lazy_core import gen_plasma_functions
                _, disconnect, psave, pload = gen_plasma_functions(Globs.PLASMA_DB)

                psave(Resources.PlasmaKeys.GROUP_STATS, group_data)
                psave(Resources.PlasmaKeys.STOCK_DATA, stock_data)
                psave(Resources.PlasmaKeys.MARKET_DATA, market_data)
                psave(Resources.PlasmaKeys.INDEX_OHLC, index_data)
                psave(Resources.PlasmaKeys.INDEX_DAILY_OHLC, index_daily_data)
                psave(Resources.PlasmaKeys.STOCK_CLASSIFICATION, stocks_classified)
                
                disconnect()
            else:
                CREATE_SAMPLE = True

                Adapters.SaveDataToPlasma.save_index_daily_ohlcv_to_plasma(CREATE_SAMPLE)
                Adapters.SaveDataToPlasma.save_stock_data_to_plasma(CREATE_SAMPLE)
                Adapters.SaveDataToPlasma.save_group_data_to_plasma(CREATE_SAMPLE)
                Adapters.SaveDataToPlasma.save_index_ohlcv_to_plasma(CREATE_SAMPLE)
                Adapters.SaveDataToPlasma.save_market_data_to_plasma(CREATE_SAMPLE)
                Adapters.classify_stocks_and_save_to_plasma(CREATE_SAMPLE)

            redis_handler = RedisHandler()
            redis_handler.delete_keys_by_pattern("pslab/stockcount/*")

    @staticmethod 
    def load_groups_and_stocks_data_from_plasma(
        required_stats: list = None, 
        groups_and_stocks: list = None, 
        load_sample = False
    ) -> pd.DataFrame:
        
        def test(): 
            required_stats = ['bu', 'sd']
            groups_and_stocks = ['HPG', 'SSI', 'VPB']
            from_day = '2024_10_01'
            to_day = '2025_10_25'

        
        # stocks = [i for i in groups_and_stocks if i in Globs.STOCKS]
        if groups_and_stocks == ['All']:
            return Adapters.load_group_data_from_plasma(required_stats)

        true_stocks = []
        if groups_and_stocks is not None:
            for s in groups_and_stocks:
                if s in Globs.SECTOR_DIC.keys():
                    true_stocks += Globs.SECTOR_DIC[s]
                elif s in Globs.STOCKS:
                    true_stocks.append(s)

        true_stocks = [i for i in true_stocks if i in Globs.STOCKS]
        df = Adapters.load_stock_data_from_plasma(required_stats, true_stocks, load_sample)
        return df

    @staticmethod
    def load_group_data_from_plasma(required_stats: list = None, groups: list=None, load_sample:bool = False):
        key = Resources.PlasmaKeys.GROUP_STATS

        if not load_sample:
            from danglib.lazy_core import gen_plasma_functions
            _, disconnect, psave, pload = gen_plasma_functions(Globs.PLASMA_DB)

            df: pd.DataFrame = pload(key)

            disconnect()
        else:
            df = FileHandler.read_parquet(f"{Resources.SAMPLE_DATA_FOLDER}/{key}.parquet")

        if required_stats is not None:
            df = df[required_stats]

        if groups is not None:
            df = df.loc[:, (slice(None), groups)]

        return df

    @staticmethod
    def load_index_ohlcv_from_plasma(name: str = None, load_sample:bool = False):
        key = Resources.PlasmaKeys.INDEX_OHLC

        if not load_sample:
            from danglib.lazy_core import gen_plasma_functions
            _, disconnect, psave, pload = gen_plasma_functions(Globs.PLASMA_DB)

            df: pd.DataFrame = pload(key)

            disconnect()
        else:
            df: pd.DataFrame = FileHandler.read_parquet(f"{Resources.SAMPLE_DATA_FOLDER}/{key}.parquet")

        day = df['day']

        if name is not None:
            df = df[[c for c in df.columns if name in c]].copy()
            df.columns = [c.split('_')[1] for c in df.columns]
        
        df['day'] = day

        return df

    @staticmethod
    def load_stock_data_from_plasma(required_stats: list = None, stocks: list = None, load_sample:bool = False):
        
        key = Resources.PlasmaKeys.STOCK_DATA

        if not load_sample:
            from danglib.lazy_core import gen_plasma_functions
            _, disconnect, psave, pload = gen_plasma_functions(Globs.PLASMA_DB)

            df: pd.DataFrame = pload(key)

            disconnect()
        else:
            df: pd.DataFrame = FileHandler.read_parquet(f"{Resources.SAMPLE_DATA_FOLDER}/{key}.parquet")

        if required_stats is not None:
            df = df[required_stats]

        if stocks is not None:
            df = df.loc[:, (slice(None), stocks)]

        return df

    @staticmethod
    def load_market_stats_from_plasma(required_stats: list = None, load_sample:bool = False):
        key = Resources.PlasmaKeys.MARKET_DATA

        if not load_sample:

            from danglib.lazy_core import gen_plasma_functions
            _, disconnect, psave, pload = gen_plasma_functions(Globs.PLASMA_DB)

            df: pd.DataFrame = pload(key)

            disconnect()
        else:
            df: pd.DataFrame = FileHandler.read_parquet(f"{Resources.SAMPLE_DATA_FOLDER}/{key}.parquet")

        if required_stats is not None:
            df = df[required_stats]

        return df

    @staticmethod
    def load_daily_stock_data_from_plasma(stocks: list = None):
        def test():
            stocks = ['VNINDEX', 'VN30']

        from danglib.lazy_core import gen_plasma_functions
        _, disconnect, psave, pload = gen_plasma_functions(db=5)
        
        nummeric_data = pload("stocks_data_nummeric")
        stocks_i2s = pickle.loads(r.get("pylabview_stocks_i2s"))
        columns = pickle.loads(r.get("pylabview_stocks_data_columns"))

        df = pd.DataFrame(nummeric_data, columns=columns)
        df['stock'] = df['stock'].map(stocks_i2s)
        df['day'] = df['day'].astype(int).astype(str).apply(lambda x: f"{x[0:4]}_{x[4:6]}_{x[6:8]}")
        disconnect()

        return df[df['stock'].isin(stocks)]
    
    @staticmethod
    def load_index_daily_ohlcv_from_plasma(required_stats: list = None, load_sample: bool = False):
        key = Resources.PlasmaKeys.INDEX_DAILY_OHLC
        try:
            if not load_sample:
                from danglib.lazy_core import gen_plasma_functions
                _, disconnect, psave, pload = gen_plasma_functions(Globs.PLASMA_DB)

                df = pload(key)
            else:
                df: pd.DataFrame = FileHandler.read_parquet(f"{Resources.SAMPLE_DATA_FOLDER}/{key}.parquet")

            if required_stats:
                df = df[required_stats]

            return df
        finally:
            disconnect()
    
    @staticmethod
    def load_sectors_stocks_from_db(stocks: list = None):
        import ast
        
        def_stocks = Globs.STOCKS if stocks is None else stocks
        
        db = MongoClient("localhost", 27022)["pylabview_db"]
        col = db['watchlist']
        df_raw = pd.DataFrame(col.find({},{'_id':0}))
        df_raw = df_raw[df_raw.index < 20]
        sectors = dict(zip(df_raw['watchlist_name'], df_raw['watchlist_params'].map(ast.literal_eval)))

        data_list = []
        for sector, stocks in sectors.items():
            for stock in stocks:
                if stock in def_stocks:
                    data_list.append({'stock': stock, 'sector': sector})

        df = pd.DataFrame(data_list)

        for stock in def_stocks:
            if stock not in df['stock'].tolist():
                data_list.append({'stock':stock, 'sector':'other'})

        df = pd.DataFrame(data_list)
        
        return df
    
    @staticmethod
    def load_sector_stocks_data_from_db():
        
        db = MongoClient(port=27022)['stockdata']
        collection = db["dc_sector_mapping"] 
        df_raw = pd.DataFrame(list(collection.find({}, {'_id': 0})))
        sector_dic = df_raw.groupby('L2')['Ticker'].apply(list).to_dict()
        
        return sector_dic
    
    @staticmethod
    def get_marketcap_stocks(symbol):

        import requests
        IBOARD_API = 'https://iboard-query.ssi.com.vn/v2'
        # path = '/stock/group/VNSML'
        headers = {
            "user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
            "Host":"iboard-query.ssi.com.vn",
            "Origin": "https://iboard.ssi.com.vn",
        }

        resp = requests.get(IBOARD_API + f'/stock/group/{symbol}', headers=headers)
        data = resp.json()
        stocks = []
        
        if data['code'] == 'SUCCESS':
            for item in data['data']:
                if 'ss' in item:
                    stocks.append(item['ss'])
        return stocks

    @staticmethod
    def classify_stocks_and_save_to_plasma(create_sample: bool = False):

        def _classify_by_sector():
            Globs.SECTOR_DIC.update(Adapters.load_sector_stocks_data_from_db())

            sector_dic = {k: v for k, v in Globs.SECTOR_DIC.items() if k not in ['Super High Beta', 'High Beta', 'Medium', 'Low']}
            group_dic = {}
            for k, v in sector_dic.items():
                for s in v:
                    if s not in group_dic:
                        group_dic[s] = [k]
                    else:
                        ls: list = group_dic[s]
                        ls.append(k)
                        group_dic[s] = ls

            res = {k: ','.join(v) for k, v in group_dic.items()}

            df = pd.DataFrame.from_dict(res, orient='index', columns=['sector'])
            df.index.name = 'stock'
            df = df[df.index.isin(Globs.STOCKS)]
            df = df.reset_index()
            return df
        
        def _classify_by_beta():
            sector_dic = {k: v for k, v in Globs.SECTOR_DIC.items() if k in ['Super High Beta', 'High Beta', 'Medium', 'Low']}
            group_dic = {}
            for k, v in sector_dic.items():
                for s in v:
                    if s not in group_dic:
                        group_dic[s] = [k]
                    else:
                        ls: list = group_dic[s]
                        ls.append(k)
                        group_dic[s] = ls

            res = {k: ','.join(v) for k, v in group_dic.items()}

            df = pd.DataFrame.from_dict(res, orient='index', columns=['beta'])
            df.index.name = 'stock'
            df = df[df.index.isin(Globs.STOCKS)]
            df = df.reset_index()
            return df

        def _classify_by_marketcap():

            classify_types = ['VNSML', 'VNMID', 'VN30']

            df = pd.DataFrame(Globs.STOCKS, columns=['stock'])

            dic = {}
            for t in classify_types:
                for i in Adapters.get_marketcap_stocks(t):
                    dic[i] = t

            df["marketCapIndex"] = df['stock'].map(dic)
            df['marketCapIndex'] = df['marketCapIndex'].fillna('other')

            return df[['stock', 'marketCapIndex']]

        dfs = [
            _classify_by_sector(),
            _classify_by_beta(),
            _classify_by_marketcap()
        ]
        df = reduce(lambda left, right: pd.merge(left, right, on='stock'), dfs)

        from danglib.lazy_core import gen_plasma_functions
        _, disconnect, psave, pload = gen_plasma_functions(Globs.PLASMA_DB)

        key = Resources.PlasmaKeys.STOCK_CLASSIFICATION

        psave(key, df)

        disconnect()

        if create_sample:
            FileHandler.write_parquet(f"{Resources.SAMPLE_DATA_FOLDER}/{key}.parquet", df)
    
        return df
    
    @staticmethod
    def load_stock_classification_from_plasma(load_sample: bool = False):
        key = Resources.PlasmaKeys.STOCK_CLASSIFICATION

        if not load_sample:
            from danglib.lazy_core import gen_plasma_functions
            _, disconnect, psave, pload = gen_plasma_functions(Globs.PLASMA_DB)

            df = pload(key)

            disconnect()
        else:
            df: pd.DataFrame = FileHandler.read_parquet(f"{Resources.SAMPLE_DATA_FOLDER}/{key}.parquet")

        return df

    class RedisAdapters:
        @staticmethod
        def load_realtime_stock_data_from_redis(r: StrictRedis=None, day=None, start=0, end=-1):
            if r is None:
                r = StrictRedis(decode_responses=True)

            if day is None:
                day = Adapters.get_max_datatime()

            key = Resources.RedisKeys.get_hose500_realtime_key(day)
            df = pd.DataFrame(json.loads(x) for x in r.lrange(key, start, end))
            return df
        
        @staticmethod
        def load_realtime_PS_data_from_redis(r: StrictRedis=None, day=None, start=0, end=-1):
            if r is None:
                r = StrictRedis(decode_responses=True)

            if day is None:
                day = datetime.now().strftime("%Y_%m_%d")

            key = Resources.RedisKeys.get_PS_realtime_key(day)
            df = pd.DataFrame(json.loads(x) for x in r.lrange(key, start, end))
            return df
        
        @staticmethod
        def load_realtime_Index_data_from_redis(r: StrictRedis=None, day=None, start=0, end=-1):
            if r is None:
                r = StrictRedis(decode_responses=True)

            if day is None:
                day = datetime.now().strftime("%Y_%m_%d")

            key = Resources.RedisKeys.get_index_realtime_key(day)
            df = pd.DataFrame(json.loads(x) for x in r.lrange(key, start, end))
            return df
            
            
        
    @staticmethod
    def get_historical_day_by_index(index):
        df = Adapters.load_index_daily_ohlcv_from_plasma()
        return df.iloc[index].name


def fix_fBuySell_data():
    pickle_path = Resources.get_aggregated_data('30S', '.pickle')
    df_raw, smap = pd.read_pickle(pickle_path)
    df_raw['stock'] = df_raw['stock'].map(smap)
    df_raw = df_raw.rename(columns={"fBuyVol": "fBuyVal", "fSellVol": "fSellVal"})
    from danglib.utils import flatten_columns


    data_path = Resources.get_aggregated_data('30S', '.parquet')
    pivot_df = df_raw.pivot(index = 'candleTime', columns='stock')
    pivot_df = flatten_columns(pivot_df)
    FileHandler.write_parquet(data_path, pivot_df)

    df_old = pd.read_parquet(data_path)
    df_old2 = pd.read_parquet(data_path)

    Adapters.SaveDataToPlasma.save_stock_data_to_plasma()

    redis_handler = RedisHandler()
    redis_handler.delete_keys_by_pattern("pslab/stockcount/*")


Globs.SECTOR_DIC.update(Adapters.load_sector_stocks_data_from_db())