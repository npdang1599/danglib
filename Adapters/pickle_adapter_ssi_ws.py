from dc_server.lazy_core import tik, tok, Tc, chunks
from functools import reduce
import pickle, pandas as pd
import numpy as np

# 'floating', 'foreignRoom'
class SESSIONS:
    DIC_SESSIONS =   {np.NaN: np.NaN, '': 0, 'PRE': 1, 'ATO': 2, 'LO': 3, 'Break': 4, 'ATC': 5, 'PT': 6, 'C': 7}
    DIC_SESSIONS_R = {-1: np.NaN,     0: '', 1: 'PRE', 2: 'ATO', 3: 'LO', 4: 'Break', 5: 'ATC', 6: 'PT', 7: 'C', 'PRE': 1}
    NaN = -1
    PRE = 1
    ATO = 2
    LO = 3
    BREAK = 4
    ATC = 5
    PT = 6
    C = 7


class STR_TO_INT:
    @staticmethod
    def int_time_to_str(t):
        if t is None or t == np.NaN: return np.NaN
        s = str(int(t))[-6:]
        return f'{s[:2]}:{s[2:4]}:{s[4:6]}'


    @staticmethod
    def maybe_int(x):
        # noinspection PyBroadException
        try:
            return int(x)
        except Exception:
            return np.NaN

    @staticmethod
    def maybe_rev_session(x):
        # noinspection PyBroadException
        try:
            return SESSIONS.DIC_SESSIONS_R[x]
        except Exception:
            return np.NaN


class PICKLED_WS_ADAPTERS:
    DES_MONGO_DB = 'ws'

    @staticmethod
    def test_adapter(MAX_NUM_COLL=20):
        from time import time
        lst = PICKLED_WS_ADAPTERS.list_all_hose500_env_collections()
        print(lst)
        count = 0
        for day in reversed(lst):
            count += 1
            if count >= MAX_NUM_COLL: break
            try:
                print(f'============================ {day} ============================')
                start = time()
                df = PICKLED_WS_ADAPTERS.read_pickled_ssi_ws_hose500_from_mongodb(day, do_restore=False, do_return_df_only=True)

                print(f'load_time: {time()-start:.2f} second(s), length: {len(df):,} data-points')
                print(f"number of stocks: {df['stock'].nunique()}")
            except Exception as e:
                print(f'Erred: {e}')
        print("testing done!")

    @staticmethod
    def converter_temp_vn30_to_df_ssi_ws(DAY, filter_volume=False):
        import pandas as pd
        from dc_server.lazy_core import Cs, copy, np
        from dc_server.sotola import FRAME
        from dc_server.adapters.pickled_adapters import PICKLED_ADAPTERS
        dic = PICKLED_ADAPTERS.read_pickled_vn30_from_mongodb(DAY)
        df2 = pd.DataFrame(dic['data'])
        df3 = copy(df2[df2['time'].isin(FRAME.timeToIndex)]).rename(columns={'time': 't'})
        df3['time'] = df3['t'].map(lambda x: 10_00_00_00 + int(x.replace(':', '')))
        STOCKS = sorted(df3['stock'].unique())
        dic_stock_to_code = {k: v for k, v in zip (STOCKS, range(len(STOCKS)))}
        df3['code'] = df3['stock'].map(lambda x: dic_stock_to_code[x])
        df3 = df3.rename(columns={
            'bid1': 'bestBid1',
            'bid2': 'bestBid2',
            'bid3': 'bestBid3',
            'bid1Vol': 'bestBid1Volume',
            'bid2Vol': 'bestBid2Volume',
            'bid3Vol': 'bestBid3Volume',
            'offer1': 'bestOffer1',
            'offer2': 'bestOffer2',
            'offer3': 'bestOffer3',
            'offer1Vol': 'bestOffer1Volume',
            'offer2Vol': 'bestOffer2Volume',
            'offer3Vol': 'bestOffer3Volume',
            'TC': 'refPrice',
            'vol': 'matchingVolume',
            'price': 'last',
            'totalVol': 'totalMatchVolume',
            'nnBuy': 'foreignerBuyVolume',
            'nnSell': 'foreignerSellVolume'
        })
        df3 = df3.groupby(['code', 'time']).last()
        df3['matchingVolume'] = df3.groupby(level='code').agg({'totalMatchVolume': 'diff'}).fillna(0)
        df3 = copy(df3[df3['matchingVolume'] > 0]) if filter_volume else copy(df3)
        #### matchedBy ####
        df3['matchedBy'] = df3.apply(
            lambda row: 1 if row['last'] == row['bestOffer1'] else -1 if row['last'] == row['bestBid1'] else np.NaN,
            axis=1)
        df3['priceDiff'] = df3['last'].groupby(level='code').diff().fillna(0)
        df3.loc[(~df3['matchedBy'].isin([-1, 1])) & (df3['priceDiff'] > 0), 'matchedBy'] = 1
        df3.loc[(~df3['matchedBy'].isin([-1, 1])) & (df3['priceDiff'] < 0), 'matchedBy'] = -1
        df3['matchedBy'] = df3['matchedBy'].groupby(level='code').fillna(method='ffill')
        df3['matchedBy'] = df3['matchedBy'].fillna(0)
        del df3['priceDiff']
        df3 = df3.reset_index().groupby(['matchedBy', 'code', 'time']).last()
        df3['x'] = df3['t'].map(lambda x: FRAME.timeToIndex[x])
        return df3

    @staticmethod
    def get_target_pickled_db():
        from pymongo import MongoClient
        client = MongoClient(PICKLED_WS_ADAPTERS.DES_MONGO_DB, 27022, serverSelectionTimeoutMS=1000)
        db_ssi_ws_hose500 = client['ssi_ws_hose500']
        return db_ssi_ws_hose500

    @staticmethod
    def list_all_hose500_env_collections():
        from dc_server.lazy_core import DB_ENV_HOSE500
        return sorted(PICKLED_WS_ADAPTERS.get_target_pickled_db().client[DB_ENV_HOSE500].list_collection_names())

    @staticmethod
    def write_binary_to_mongodb(data, db_name='', col_name='test', do_drop=True, verbosity=1, not_temp_vn30=False):
        from bson import Binary
        from bson.objectid import ObjectId
        db_ssi_ws_hose500 = PICKLED_WS_ADAPTERS.get_target_pickled_db()
        client = db_ssi_ws_hose500.client
        if db_name != '': db_ssi_ws_hose500 = client[db_name]
        col = db_ssi_ws_hose500[col_name]
        if verbosity >= 2: tik()
        if do_drop: col.drop()
        if len(data) > 16_000_001:
            for chunk in chunks(data, 16_000_000):
                res = col.insert_one({'pickledData': Binary(chunk)})
                assert type(res.inserted_id) == ObjectId, 'Loi: write_binary_to_mongodb()'
                print(f'Inserted into {Tc.CRED2}{client.address}["{db_ssi_ws_hose500.name}"]["{col.name}"]{Tc.CEND}: {res.inserted_id}')
        else:
            res = col.insert_one({'pickledData': Binary(data)})
            assert type(res.inserted_id) == ObjectId, 'Loi: write_binary_to_mongodb()'
            print(f'Inserted into {Tc.CRED2}{client.address}["{db_ssi_ws_hose500.name}"]["{col.name}"]{Tc.CEND}: {res.inserted_id}')
        if verbosity >= 2: print(f'Successfully wrote {Tc.CYELLOW2}{len(data)/1048576:,.2f} MB{Tc.CEND} into db: {client.address}["{db_ssi_ws_hose500.name}"]["{col.name}"]')
        if verbosity >= 2: tok()


    @staticmethod
    def list_all_hose500_collections(verbosity=0):
        db_ssi_ws_hose500 = PICKLED_WS_ADAPTERS.get_target_pickled_db()
        client = db_ssi_ws_hose500.client
        if verbosity >= 1: print(sorted(db_ssi_ws_hose500.list_collection_names()))
        return sorted(db_ssi_ws_hose500.list_collection_names())

    @staticmethod
    def list_all_vn30_collections():
        client= PICKLED_WS_ADAPTERS.get_target_pickled_db().client
        db = client['ssi_ws_vn30']
        print(sorted(db.list_collection_names()))
        return sorted(db.list_collection_names())

    @staticmethod
    def write_ssi_ws_vn30_to_mongodb(data, col_name='test', do_drop=True, verbosity=1, not_temp_vn30=False):
        from bson import Binary
        from bson.objectid import ObjectId
        client = PICKLED_WS_ADAPTERS.get_target_pickled_db().client
        db = client['ssi_ws_vn30']
        col = db[col_name]
        if verbosity >= 2: tik()
        if do_drop: col.drop()
        if len(data) > 16_000_001:
            for chunk in chunks(data, 16_000_000):
                res = col.insert_one({'pickledData': Binary(chunk)})
                assert type(res.inserted_id) == ObjectId, 'Loi: write_binary_to_mongodb()'
                print(f'Inserted into {Tc.CRED2}{client.address}["{db.name}"]["{col.name}"]{Tc.CEND}: {res.inserted_id}')
        else:
            res = col.insert_one({'pickledData': Binary(data)})
            assert type(res.inserted_id) == ObjectId, 'Loi: write_binary_to_mongodb()'
            print(f'Inserted into {Tc.CRED2}{client.address}["{db.name}"]["{col.name}"]{Tc.CEND}: {res.inserted_id}')
        if verbosity >= 2: print(f'Successfully wrote {Tc.CYELLOW2}{len(data)/1048576:,.2f} MB{Tc.CEND} into db: {client.address}["{db.name}"]["{col.name}"]')
        if verbosity >= 2: tok()

    @staticmethod
    def load_hose500_from_db(col_name, do_return_df_only=False):
        def test():
            col_name = "2024_10_11"
            do_return_df_only = False
        print(f"\x1b[90mPICKLED_WS_ADAPTERS.load_hose500_from_db: \x1b[93m{col_name}\x1b[0m", end=" ")
        print('...', end='')
        dic = PICKLED_WS_ADAPTERS.read_pickled_ssi_ws_hose500_from_mongodb(
            col_name=col_name,
            do_restore=False,
            do_return_df_only=do_return_df_only)
        print(" Done!")
        return dic

    @staticmethod
    def read_pickled_ssi_ws_hose500_from_mongodb(col_name, verbosity=1, do_restore=True, restore_cols=None,
                                                 do_return_df_only=True, not_temp_vn30=False):
        
        def test():
            col_name = "2024_10_11"
            verbosity=1
            do_restore=True
            restore_cols=None
            do_return_df_only=True
            not_temp_vn30=False

        import zlib
        db_pickled = PICKLED_WS_ADAPTERS.get_target_pickled_db()
        col = db_pickled[col_name]
        if verbosity >= 2: tik(); print(f'   Loading {col_name} from db')
        df_zip = pd.DataFrame(col.find({}))
        zipped_binary_data = reduce(
            lambda a, b: a + b,
            df_zip.sort_values('_id')['pickledData'])

        if verbosity >= 2: print('   Unzipping...')
        dic = pickle.loads(zlib.decompress(zipped_binary_data))
        if verbosity >= 2: tok(); print(dic.keys())
        df = pd.DataFrame(dic['data'])
        if do_restore:
            if verbosity >= 2: print('   restoring string columns')
            if restore_cols is None: restore_cols = ['time', 'session', 'code']
            if 'time' in restore_cols: df['time'] = df['time'].map(STR_TO_INT.int_time_to_str)
            if 'session' in restore_cols: df['session'] = df['session'].map(STR_TO_INT.maybe_int).map(STR_TO_INT.maybe_rev_session)
            if 'code' in restore_cols:
                try: df['code'] = df['id'].map(lambda x: dic['id_to_stock'][x])
                except Exception as e: print(e)
        if verbosity >= 2: print(f'read_pickled_ssi_ws_hose500_from_mongodb: Done loading {col_name}!')
        if do_return_df_only: return df
        return dic

    @staticmethod
    def read_pickled_ssi_ws_vn30_from_mongodb(col_name, verbosity=1, do_restore=True, restore_cols=None, do_return_df_only=True, not_temp_vn30=False):
        import zlib
        client = PICKLED_WS_ADAPTERS.get_target_pickled_db().client
        db_pickled = client['ssi_ws_vn30']
        col = db_pickled[col_name]
        if verbosity >= 2: tik(); print(f'   Loading {col_name} from db')
        zipped_binary_data = reduce(lambda a, b: a + b, map(lambda x: x['pickledData'], col.find({}, {'_id': 0})))
        if verbosity >= 2: print('   Unzipping...')
        dic = pickle.loads(zlib.decompress(zipped_binary_data))
        if verbosity >= 2: tok(); print(dic.keys())
        df = pd.DataFrame(dic['data'])

        if do_restore:
            if verbosity >= 2: print('   restoring string columns')
            if restore_cols is None: restore_cols = ['time', 'session', 'code']
            if 'time' in restore_cols: df['time'] = df['time'].map(STR_TO_INT.int_time_to_str)
            if 'session' in restore_cols: df['session'] = df['session'].map(STR_TO_INT.maybe_int).map(STR_TO_INT.maybe_rev_session)
            if 'code' in restore_cols: df['code'] = df['id'].map(lambda x: dic['id_to_stock'][x])
        if verbosity >= 2: print(f'read_pickled_ssi_ws_hose500_from_mongodb: Done loading {col_name}!')
        if do_return_df_only: return df
        return dic


def extract_and_zip_df_vn30(DAY):
    import zlib
    dic = PICKLED_WS_ADAPTERS.read_pickled_ssi_ws_hose500_from_mongodb(DAY, restore_cols=[], do_return_df_only=False)
    VN30_ID = [ 233,  580,   29,  974, 3825, 2557,   32, 1354,  436, 2028, 1386, 2908, 2200, 2920,   25, 1152, 2027,  209, 2024, 3801,  217, 1734, 2947, 3852, 3854,   66, 2576, 2567, 3813, 3847]
    df = pd.DataFrame(dic['data'])
    df = df[df['id'].isin(VN30_ID)]
    dic['data'] = df
    zipped_binary_data = zlib.compress(pickle.dumps(dic))
    print(f'{len(zipped_binary_data)/ 2**20:,.2f} mb')
    return zipped_binary_data

#%%

CONVERT_FROM_HOSE_500_TO_VN_30 = True
if __name__ == '__main__' and CONVERT_FROM_HOSE_500_TO_VN_30:
    lst_vn30 = PICKLED_WS_ADAPTERS.list_all_vn30_collections()
    for DAY in PICKLED_WS_ADAPTERS.list_all_hose500_collections():
        if DAY in lst_vn30: continue
        print(DAY)
        tik()
        zipped_binary_data = extract_and_zip_df_vn30(DAY)
        PICKLED_WS_ADAPTERS.write_ssi_ws_vn30_to_mongodb(zipped_binary_data, col_name=DAY)
        tok()

#%%
GENERATE_MONGODB_ALL_DAYS = False
if __name__ == '__main__' and GENERATE_MONGODB_ALL_DAYS:
    print(PICKLED_WS_ADAPTERS.list_all_hose500_collections())
    DAY = '2021_12_10'
    tik()
    dic = PICKLED_WS_ADAPTERS.read_pickled_ssi_ws_hose500_from_mongodb('2021_12_10', restore_cols=[], do_return_df_only=False)
    tok()