from pymongo import MongoClient
from redis import StrictRedis
import pandas as pd
from datetime import datetime as dt
from danglib.Adapters.pickle_adapter_ssi_ws import PICKLED_WS_ADAPTERS
import json

class MongoCLIs:
    @staticmethod
    def ws_27022():
        cli = MongoClient(
            host='ws', 
            port=27022,
            connectTimeoutMS=2000,
            serverSelectionTimeoutMS=2000)
        return cli
    
    @staticmethod
    def lv2_27022():
        cli = MongoClient(
            host='lv2',
            port=27022,
            connectTimeoutMS=2000,
            serverSelectionTimeoutMS=2000)
        return cli

    @staticmethod
    def cmc_27017():
        cli = MongoClient(
            host='cmc',
            port=27017,
            connectTimeoutMS=2000,
            serverSelectionTimeoutMS=2000)
        return cli
    
    @staticmethod
    def cmc_27022():
        cli = MongoClient(
            host='cmc',
            port=27022,
            username='admin', 
            password='Adm1n$$F5tr@der&', 
            authSource='admin',
            connectTimeoutMS=2000,
            serverSelectionTimeoutMS=2000)
        return cli
    
    @staticmethod
    def ws_27021():
        cli = MongoClient(
            host='113.161.34.115', 
            port=27021,
            connectTimeoutMS=2000,
            serverSelectionTimeoutMS=2000)
        return cli
    

class RedisClis:
    @staticmethod
    def ws():
        cli = StrictRedis('ws', 6379)
        return cli
    
    @staticmethod
    def lv2():
        cli = StrictRedis('lv2', 6379)
        return cli


class MongoDBs:
    @staticmethod
    def ws_curated():
        cli = MongoCLIs.ws_27022()
        db = cli['curated']
        return db

    @staticmethod
    def lv2_dc_data():
        cli = MongoCLIs.lv2_27022()
        db = cli['dc_data']
        return db

    @staticmethod
    def cmc_dc_data():
        cli = MongoCLIs.cmc_27017()
        db = cli['dc_data']
        return db
    
    @staticmethod
    def cmc_curated():
        cli = MongoCLIs.cmc_27022()
        db = cli['curated']
        return db
    
    @staticmethod
    def ws_ssi_ps_data():
        cli = MongoCLIs.ws_27022()
        db = cli['ssi_ps_data']
        return db
    
    @staticmethod
    def ws_21_weights():
        cli = MongoCLIs.ws_27021()
        db = cli['weights']
        return db   
    
    @staticmethod
    def ws_cache():
        cli = MongoCLIs.ws_27022()
        db = cli['cache']
        return db
    
    @staticmethod
    def hose_500(day):
        db = PICKLED_WS_ADAPTERS.load_hose500_from_db(day)
        return db
    

class MongoAdapters:

    @staticmethod
    def load_ps_premium_from_db(day=None, return_list_collection=False):
        db = MongoDBs.cmc_curated()
        if return_list_collection:
            col_ls = sorted([col for col in db.list_collection_names() if '_premium' in col])
            return col_ls 
        
        if day is None:
            col_ls = [col for col in db.list_collection_names() if '_premium' in col]
            col = db[max(col_ls)]
        else:
            col = db[f'{day}_premium']
            
        query = col.find({'desc':'premium'}, {'_id':0})
        data = pd.DataFrame(list(query)[0])
        return data
    
    @staticmethod
    def load_ssi_VN30_data_from_db(day):
        # day = '2023_01_10'
        db = MongoDBs.cmc_curated()
        coll_name = f'{day}_KIS'

        data = list(db[coll_name].find({}, {"_id": 0}))
        list_df = [pd.DataFrame(data[i]) for i in range(len(data))]
        df = pd.concat(list_df,ignore_index=True)
        return df

    @staticmethod
    def load_ssi_hose_eod_from_db(day, stock_lst=None):
        db = MongoDBs.cmc_dc_data()
        sorted([i for i in db.list_collection_names() if '_SSI_hose' in i])
        coll_name = f"{day}_SSI_hose"
        stock_query = {'title':{'$in':stock_lst}} if stock_lst is not None else {}
        data = list(db[coll_name].find(stock_query, {"_id": 0}))
        list_df = [pd.DataFrame(data[i]) for i in range(len(data))]
        df = pd.concat(list_df,ignore_index=True)
        return df
    
    @staticmethod
    def load_vps_hose_eod_from_db(day, stock_lst=None):
        db = MongoDBs.cmc_dc_data()
        sorted([i for i in db.list_collection_names() if '_VPS_hose' in i])
        coll_name = f"{day}_VPS_hose"
        # stock_query = {}
        stock_query = {'ticker':{'$in':stock_lst}} if stock_lst is not None else {}
        data = list(db[coll_name].find(stock_query, {"_id": 0}))
        list_df = [pd.DataFrame(data[i]) for i in range(len(data))]
        df = pd.concat(list_df,ignore_index=True)
        return df

    @staticmethod
    def adapter_read_from_temp_all_hose(day='2022_12_29'):
        TEMP_VN30_COLUMNS = {
            0: 'stock',
            1: 'TC',
            2: 'ceiling',
            3: 'floor',
            4: 'bid3',
            5: 'bid3Vol',
            6: 'bid2',
            7: 'bid2Vol',
            8: 'bid1',
            9: 'bid1Vol',
            10: 'price',
            11: 'vol',
            13: 'offer1',
            14: 'offer1Vol',
            15: 'offer2',
            16: 'offer2Vol',
            17: 'offer3',
            18: 'offer3Vol',
            19: 'totalMatchVolume',
            22: 'TB',
            23: 'high',
            24: 'low',
            25: 'nnBuy',
            26: 'nnSell',
            27: 'time'
        }

        db = MongoDBs.lv2_dc_data()
        data = db[f'{day}_temp_all_hose']\
                    .find({})\
                    .sort('_id', -1)\
                    .limit(1)\
                    .next()
        df = pd.DataFrame(data['req']).rename(columns=TEMP_VN30_COLUMNS)
        df['time'] = dt.fromtimestamp(data['time']['stamp']/1000)
        df = df[['time'] + [x for x in df.columns if x not in ['time']]]
        df['price'] = (df['price'].map(float) * 1000).astype(int)
        return df[:]
    
    @staticmethod
    def load_F1_from_db(day):

        db = MongoDBs.cmc_curated()
        coll_name = f'{day}_f1'

        df = pd.DataFrame(list(db[coll_name].find({}, {"_id": 0}))[0])
        return df
    
    @staticmethod
    def load_ps_data_from_db(type="F1", day="2022_10_10"):
        """
        input: 
        - type <str> : F1, F2 or F3
        - day <str> : date in 2022
        output:
        - pd.DataFrame : tick data of F1, F2 or F3
        """

        db = MongoDBs.ws_ssi_ps_data()
        col = db[f'{day}_{type.lower()}']   
        data = list(col.find({},{"_id":0}))
        return pd.DataFrame(data)
    
    @staticmethod
    def load_hose500_from_db(day):
        db = MongoDBs.hose_500(day)
        df = pd.DataFrame.from_dict(db.get("data"))
        return df
    
    @staticmethod
    def load_vn30_weight_from_db(day):
        # db = MongoClient('ws', 27021)['weights']
        db = MongoDBs.ws_21_weights()
        coll = db['vn30_weights']
        query = {} if day is None else {'day':day}
        df = pd.DataFrame(list(coll.find(query,{'_id':0})))
        return df


class RedisAdapters:
    @staticmethod
    def get_vn30_ls(day=None):
        if day is None or day >= "2023_08_07":
            VN30_LIST = ['ACB', 'BCM', 'BID', 'BVH', 'CTG', 'FPT', 'GAS', 'GVR', 'HDB', 'HPG', 
                        'MBB', 'MSN', 'MWG', 'SHB', 'SSB', 'PLX', 'POW', 'SAB', 'SSI', 'STB', 
                        'TCB', 'TPB', 'VCB', 'VHM', 'VIB', 'VIC', 'VJC', 'VNM', 'VPB', 'VRE']
            try:
                r = StrictRedis()
                VN30_LIST = list(json.loads(r.get("redis.vn30_list")).values())
            except:
                pass

        elif day >= "2023_02_06":
            VN30_LIST = ['ACB', 'BCM', 'BID', 'BVH', 'CTG', 'FPT', 'GAS', 'GVR', 'HDB', 'HPG', 
                        'MBB', 'MSN', 'MWG', 'NVL', 'PDR', 'PLX', 'POW', 'SAB', 'SSI', 'STB', 
                        'TCB', 'TPB', 'VCB', 'VHM', 'VIB', 'VIC', 'VJC', 'VNM', 'VPB', 'VRE']
        elif '2022_08_01' <= day < "2023_02_06":
            VN30_LIST = ['ACB', 'BID', 'BVH', 'CTG', 'FPT', 'GAS', 'GVR', 'HDB', 'HPG', 'KDH',
                        'MBB', 'MSN', 'MWG', 'NVL', 'PDR', 'PLX', 'POW', 'SAB', 'SSI', 'STB',
                        'TCB', 'TPB', 'VCB', 'VHM', 'VIB', 'VIC', 'VJC', 'VNM', 'VPB', 'VRE']
        elif '2021_08_01' <= day < "2022_08_01":
            VN30_LIST = ['ACB', 'BID', 'BVH', 'CTG', 'FPT', 'GAS', 'GVR', 'HDB', 'HPG', 'KDH',
                        'MBB', 'MSN', 'MWG', 'NVL', 'PDR', 'PLX', 'PNJ', 'POW', 'SAB', 'SSI',
                        'STB', 'TCB', 'TPB', 'VCB', 'VHM', 'VIC', 'VJC', 'VNM', 'VPB', 'VRE']
        else: # truoc thang 8 thi dung list nay
            VN30_LIST = ['BID', 'BVH', 'CTG', 'FPT', 'GAS', 'HDB', 'HPG', 'KDH', 'MBB', 'MSN',
                        'MWG', 'NVL', 'PDR', 'PLX', 'PNJ', 'POW', 'REE', 'SBT', 'SSI', 'STB',
                        'TCB', 'TCH', 'TPB', 'VCB', 'VHM', 'VIC', 'VJC', 'VNM', 'VPB', 'VRE']
        
        return sorted(VN30_LIST)


# class PlasmaAdapters:
#     @staticmethod
#     def load_ssi_vn30_data_from_plasma(day, pload = None):
#         if pload is None:
#             _, _, psave, pload = gen_plasma_functions(db=1)
#         key = f'plasma.indicators.pssi_agg.pssi_dfs.{day}'
#         df = pload(key)
#         return df


def test():
    df = MongoAdapters.load_ps_data_from_db(day='2023_03_23')
    df1 = MongoAdapters.load_F1_from_db(day='2023_03_23')

