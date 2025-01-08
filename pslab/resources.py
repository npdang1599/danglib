import warnings
import logging

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
from danglib.pslab.utils import day_to_timestamp, FileHandler, unflatten_columns

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
    SAMPLE_DATA_FROM_TIMESTAMP = day_to_timestamp('2024_12_05')
    BASE_TIMEFRAME = '30S'

    MAKETSTATS_SRC = ['buyImpact', 'sellImpact', 'Arbit', 'Unwind', 'premiumDiscount', 'f1Bid', 'f1Ask']
    
    STOCKSTATS_SRC = ['open', 'high', 'low', 'close', 'matchingValue', 'bu', 'sd', 'bu2',
       'sd2', 'bid', 'ask', 'refPrice', 'fBuyVol', 'fSellVol', 'return']
    
    DAILYINDEX_SRC = ['F1Open', 'Vn30Open', 'VnindexOpen', 'F1High', 'Vn30High', 'VnindexHigh', 'F1Low', 'Vn30Low', 'VnindexLow', 'F1Close', 'Vn30Close', 'VnindexClose', 'F1Volume', 'Vn30Volume', 'VnindexVolume']
    
    STOCKS_SRC = ['bu', 'sd', 'bu2', 'sd2', 'bid', 'ask', 'fBuyVol', 'fSellVol']

    GROUP_SRC = ['ask', 'bid', 'bu', 'bu2', 'fBuyVol', 'fSellVol','sd', 'sd2']

    SECTOR_DIC = {
        'Super High Beta': ['SHS', 'VGS', 'MBS', 'VIX', 'CTS', 'ORS', 'CEO', 'FTS', 'DTD', 'AGR', 'GEX', 'BSI', 'HUT', 'VCI', 'DIG', 'VND', 'VDS', 'L14', 'DXG', 'DGW', 'PDR', 'HCM', 'CII', 'HTN', 'GVR', 'NKG', 'BVS', 'HSG', 'TCI', 'NVL', 'SSI', 'GIL', 'PXL', 'KSB', 'PLC', 'NLG', 'KBC', 'DDV', 'FCN', 'LCG', 'DPG', 'DBC', 'TCH', 'VOS', 'VPG', 'HDC', 'IDJ', 'ANV', 'VCG', 'PET', 'VGC', 'PC1', 'HAH', 'ASM'], 
        'High Beta': ['IJC', 'C4G', 'BCG', 'HHV', 'DXS', 'CSV', 'IDI', 'TNG', 'SZC', 'HHS', 'CTD', 'KHG', 'ADS', 'PVC', 'TLH', 'DCM', 'DGC', 'SCR', 'S99', 'TIG', 'MWG', 'LAS', 'PVD', 'AAA', 'PVT', 'POM', 'MSN', 'HBC', 'PVS', 'CMX', 'VTP', 'PVB', 'LSS', 'IDC', 'TIP', 'DPM', 'HDG', 'VSC', 'HQC', 'HPG', 'SMC', 'EVF', 'NTL', 'VGI', 'DRI', 'PAN', 'VHC', 'CSC', 'CNG'], 
        'Medium': ['VRE', 'EIB', 'STB', 'DSC', 'TCB', 'KDH', 'MSB', 'VHM', 'TV2', 'CTR', 'LDG', 'VGT', 'CTI', 'BSR', 'SHB', 'MSH', 'CTG', 'ELC', 'PHR', 'VIB', 'VIC', 'PSH', 'TTF', 'BFC', 'TPB', 'MSR', 'LHG', 'VCS', 'SIP', 'OCB', 'SKG', 'DPR', 'GMD', 'VPB', 'MBB', 'BID', 'APH'], 
        'Low': ['HAG', 'PLX', 'AGG', 'HAX', 'SBT', 'LPB', 'BMP', 'BCM', 'GEG', 'FPT', 'POW', 'HVN', 'PVP', 'TNH', 'DHC', 'NT2', 'OIL', 'DRC', 'BVH', 'REE', 'FRT', 'ABB', 'ACB', 'HNG', 'CMG', 'GAS', 'GSP', 'VIP', 'HDB', 'DHA', 'SAB', 'VTO', 'PNJ', 'BAF', 'QNS', 'NAF', 'YEG', 'VJC', 'VNM', 'NAB', 'PTB', 'VCB', 'ITD', 'TCM', 'VPI', 'VEA', 'SJS', 'MCH', 'SSB', 'FOX', 'ACV', 'SCS', 'BWE', 'NCT', 'KDC'],
        "VN30": ['ACB', 'BCM', 'BID', 'BVH', 'CTG', 'FPT', 'GAS', 'GVR', 'HDB', 'HPG', 'MBB', 'MSN', 'MWG', 'PLX', 'POW', 'SAB', 'SHB', 'SSB', 'SSI', 'STB', 'TCB', 'TPB', 'VCB', 'VHM', 'VIB', 'VIC', 'VJC', 'VNM', 'VPB', 'VRE'],
        "RE1":[ 'DXG', 'DIG', 'KDH', 'CEO', 'NLG'],
        "RE2":[ 'DXG', 'DIG', 'KDH', 'CEO', 'NLG', 'NVL', 'VHM'],
        "IP":[ 'KBC' ,'IDC' ,'VGC'],
        "SEC1":[ 'SSI' , 'VND' , 'HCM' , 'VCI'],
        "SEC2":[ 'SHS', 'BSI', 'FTS'],
        "Retail":[ 'MWG'  , 'DGW', 'FRT'],
        "Steel":[ 'HPG' , 'HSG' , 'NKG'],
        "SOBank":[ 'VCB' , 'BID' , 'CTG'],
        "POBank":[ 'TCB' , 'SHB' , 'VPB' , 'STB' , 'MBB'],
        "All": STOCKS,
    }

    ROLLING_TIMEFRAME = ['30s', '1Min', '5Min', '15Min', '30Min', '1H', '4H', '1D']


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
            day = '2022_04_11'

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
        def save_stock_data_to_plasma(create_sample=False):
        
            from danglib.lazy_core import gen_plasma_functions
            _, disconnect, psave, pload = gen_plasma_functions(db=10)

            df = Adapters.load_stock_data_from_parquet()

            if create_sample:
                dfs = df[df.index > Globs.SAMPLE_DATA_FROM_TIMESTAMP]
                FileHandler.write_parquet(f"{Resources.SAMPLE_DATA_FOLDER}/pslab_stock_data.parquet", dfs)

            psave("pslab_stock_data", df)

            disconnect()

        @staticmethod
        def save_market_data_to_plasma(create_sample=False):
            from danglib.lazy_core import gen_plasma_functions
            _, disconnect, psave, pload = gen_plasma_functions(db=10)

            df_market_stats = Adapters.load_market_stats_from_parquet()

            if create_sample:
                dfs = df_market_stats[df_market_stats.index > Globs.SAMPLE_DATA_FROM_TIMESTAMP]
                FileHandler.write_parquet(f"{Resources.SAMPLE_DATA_FOLDER}/pslab_market_data.parquet", dfs)

            psave("pslab_market_data", df_market_stats)

            disconnect()

        @staticmethod
        def save_index_ohlcv_to_plasma(create_sample=False):

            from danglib.lazy_core import gen_plasma_functions
            _, disconnect, psave, pload = gen_plasma_functions(db=10)

            df = Adapters.load_index_ohlc_from_pickle()

            if create_sample:
                dfs = df[df.index > Globs.SAMPLE_DATA_FROM_TIMESTAMP]
                FileHandler.write_parquet(f"{Resources.SAMPLE_DATA_FOLDER}/pslab_ohlcv_index_data.parquet", dfs)

            psave("pslab_ohlcv_index_data", df)

            disconnect()
        
        @staticmethod
        def save_group_data_to_plasma(create_sample=False):
            from danglib.lazy_core import gen_plasma_functions
            _, disconnect, psave, pload = gen_plasma_functions(db=10)

            ls = []
            for g in Globs.SECTOR_DIC.keys():
                path = Resources.get_group_stats_aggregated('30S', group=g)
                df_tmp: pd.DataFrame = pd.read_pickle(path)
                df_tmp = df_tmp.rename(columns={c:f"{c}_{g}" for c in df_tmp.columns})

                ls.append(df_tmp)
            
            df = pd.concat(ls, axis=1).sort_index()

            df = unflatten_columns(df)
            df = df.sort_index(axis=1)

            if create_sample:
                dfs = df[df.index > Globs.SAMPLE_DATA_FROM_TIMESTAMP]
                FileHandler.write_parquet(f"{Resources.SAMPLE_DATA_FOLDER}/pslab_group_stats_data.parquet", dfs)

            
            psave("pslab_group_stats_data", df)

            disconnect()

        @staticmethod
        def save_index_daily_ohlcv_to_plasma(create_sample: bool = False):

            from danglib.lazy_core import gen_plasma_functions
            _, disconnect, psave, pload = gen_plasma_functions(db=10)

            key = "pslab_daily_index_ohlcv"

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

            if create_sample:
                FileHandler.write_parquet(f"{Resources.SAMPLE_DATA_FOLDER}/{key}.parquet", dfr)

            psave(key, dfr)

            disconnect()


        @staticmethod
        def run_save_all():
            CREATE_SAMPLE = True

            Adapters.SaveDataToPlasma.save_group_data_to_plasma(CREATE_SAMPLE)
            Adapters.SaveDataToPlasma.save_index_ohlcv_to_plasma(CREATE_SAMPLE)
            Adapters.SaveDataToPlasma.save_market_data_to_plasma(CREATE_SAMPLE)
            Adapters.SaveDataToPlasma.save_stock_data_to_plasma(CREATE_SAMPLE)
            Adapters.SaveDataToPlasma.save_index_daily_ohlcv_to_plasma(CREATE_SAMPLE)



    @staticmethod 
    def load_groups_and_stocks_data_from_plasma(required_stats: list = None, groups_and_stocks: list = None, load_sample = False) -> pd.DataFrame:
        
        def test():
            required_stats = ['bu', 'sd']
            groups_and_stocks = ['VN30', 'Super High Beta', 'HPG', 'SSI']

        if groups_and_stocks is None:
            return Adapters.load_group_data_from_plasma()
        
        stocks = [i for i in groups_and_stocks if i in Globs.STOCKS]
        groups = [i for i in groups_and_stocks if i in Globs.SECTOR_DIC.keys()]

        ls = []
        if len(stocks) > 0:
            ls.append(Adapters.load_stock_data_from_plasma(required_stats, stocks, load_sample))

        if len(groups) > 0:
            ls.append(Adapters.load_group_data_from_plasma(required_stats, groups, load_sample))

        df = pd.concat(ls, axis=1).sort_index()

        return df


    @staticmethod
    def load_group_data_from_plasma(required_stats: list = None, groups: list=None, load_sample:bool = False):
        key = "pslab_group_stats_data"

        if not load_sample:
            from danglib.lazy_core import gen_plasma_functions
            _, disconnect, psave, pload = gen_plasma_functions(db=10)

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
    def load_index_ohlcv_from_plasma(name: str, load_sample:bool = False):
        key = "pslab_ohlcv_index_data"

        if not load_sample:
            from danglib.lazy_core import gen_plasma_functions
            _, disconnect, psave, pload = gen_plasma_functions(db=10)

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
        
        key = "pslab_stock_data"
        if not load_sample:
            from danglib.lazy_core import gen_plasma_functions
            _, disconnect, psave, pload = gen_plasma_functions(db=10)

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
        key = "pslab_market_data"

        if not load_sample:

            from danglib.lazy_core import gen_plasma_functions
            _, disconnect, psave, pload = gen_plasma_functions(db=10)

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
        key = "pslab_daily_index_ohlcv"

        if not load_sample:
            from danglib.lazy_core import gen_plasma_functions
            _, disconnect, psave, pload = gen_plasma_functions(db=10)

            df = pload(key)

            disconnect()
        else:
            df: pd.DataFrame = FileHandler.read_parquet(f"{Resources.SAMPLE_DATA_FOLDER}/{key}.parquet")

        if required_stats:
            df = df[required_stats]

        return df







        



       




    

    


