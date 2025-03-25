from dc_server.lazy_core import *
import pandas as pd, os
import warnings
warnings.filterwarnings('ignore')
from dc_server.sotola import FRAME
import itertools
from dc_server.ssi_ws.ssi_ws_stock_parser import BASE_PARSER, PARSER
from pymongo import MongoClient
from datetime import datetime
from dateutil import tz
from threading import Thread
from multiprocessing import Process, Manager
from dc_server.lazy_core import gen_plasma_functions
_, _, _, pload = gen_plasma_functions()

VN_TZ = tz.gettz("Asia/Ho_Chi_Minh")
db = MongoClient(host="cmc",port=27022, username='admin', password=os.getenv("CMC_MONGO_PASSWORD"), authSource='admin')["curated"]
ws = MongoClient('ws', 27021)["curated"]
import redis
r = redis.StrictRedis(host="ws", port=6379)
rcmc2 = redis.StrictRedis(host="cmc2", port=6379)
rlv2 = redis.StrictRedis(host="lv2", port=6379)

class PADAPTERS:
    @staticmethod
    def load_ssi_ws_hose500():
        current_time = dt.now(tz=VN_TZ)
        dayOfWeek = current_time.weekday()
        current_time = current_time.strftime('%X') 
        if dayOfWeek <= 4 and ('09:00:00' <= current_time < '12:35:00' or '12:44:00' <= current_time <= '15:01:00'):        
            key = PARSER.plasma_output_key()
            df = pd.DataFrame(pload(key), columns=BASE_PARSER.NUMERICAL_COLUMNS)
            # print(df)
            dic_id_to_stock = BASE_PARSER.get_ssi_id_to_stock()
            df['stock'] = df['id'].map(dic_id_to_stock)

        # df columns: 
        # 'session', 'time', 'code', 'matchedBy', 'last', 'matchingVolume', 'totalMatchVolume', 'totalMatchValue', 'foreignerBuyVolume', 
        # 'foreignerSellVolume', 'matchingValue', 'timestamp', 'refPrice', 'bestBid1', 'bestBid1Volume', 'bestBid2', 'bestBid2Volume', 
        # 'bestBid3', 'bestBid3Volume', 'bestOffer1', 'bestOffer1Volume', 'bestOffer2', 'bestOffer2Volume', 'bestOffer3', 'bestOffer3Volume', 
        # 'id', 'sequence', 'stock']

        return df

############################################ FUNCTION #########################################

def time_int_to_x1(t):
    t = str(t)
    t = t[2:4] + ':' + t[4:6] + ':' + t[6:8]
    if '11:30:59' < t < '13:00:00':
        t = '11:30:59'
    elif t > '14:45:59':
        t = '14:45:59'
    # print(FRAME.timeToIndex[t])    
    # print(t)
    t = FRAME.timeToIndex[t]
    # print(t)
    return t

def load_vn30_weights_from_db(day=None):
    db = MongoClient('ws', 27021)['weights']
    coll = db['vn30_weights']
    if day is None:
        day = datetime.now(tz=VN_TZ).strftime("%Y_%m_%d")
    df = list(coll.find({'day':{'$lte': day}},{'_id':0}).sort('day', -1).limit(30))
    if len(df) == 0:
        df = list(coll.find({'day':{'$gte': day}},{'_id':0}).sort('day', 1).limit(30))
    df = pd.DataFrame(df)
    return df

def compute_df_cut(df=None, head_length=100, upper_bound=12.0, lower_bound=8.0, bid_or_offer='bid', grid_size=5, verbosity=1, do_split_order=False, hardcoded_threshold=0):

    def test():
        df = None
        head_length=100
        upper_bound=12.0
        lower_bound=8.0
        bid_or_offer='bid'
        grid_size=5
        verbosity=1
        do_split_order=False
        hardcoded_threshold=0

    

    bid_or_offer = bid_or_offer.lower().capitalize()
    lookup = {'Bid': 1, 'Offer': -1}
    if verbosity >= 2: tik()
    if df is None: df:pd.DataFrame = PADAPTERS.load_ssi_ws_hose500()
    if len(df.index) <= 0:
        return df
    if verbosity >= 2: tok()

    ############## df_matched ##############
    def split_matched_orders_by_side():
        df_matched = df.groupby(['matchedBy', 'code', 'time']).agg({'matchingVolume': sum}).loc[lookup[bid_or_offer]]
        df_matched['matchedVolume'] = df_matched.groupby(level='code').agg({'matchingVolume': 'cumsum'})
        df_matched_lookup = df_matched.reset_index().pivot(index='time', columns='code', values='matchedVolume').fillna(method='ffill').fillna(0)
        df_matched_lookup = df_matched_lookup.unstack().to_frame().rename(columns={0: 'accMatchingVolume'})
        if verbosity >= 2: tok()
        return df_matched_lookup

    ################ df_cut ################
    df_cut:pd.DataFrame = copy(df)
    df_cut = df_cut.sort_values(['code','totalMatchVolume'])
    df_cut['real_MatchingVolume'] = df_cut['totalMatchVolume'].diff()
    df_cut['real_MatchingVolume'] = np.where(df_cut['real_MatchingVolume']<0,0,df_cut['real_MatchingVolume'])
    df_cut['real_MatchingVolume'].iloc[0] = 0
    # print(df_cut.columns)
    if bid_or_offer != 'Bid':
        df_cut[['bestBid1', 'bestBid1Volume']] = df_cut[['bestOffer1', 'bestOffer1Volume']].copy()
    df_cut['bestBid1'] = df_cut['refPrice']
    df_cut = df_cut.groupby(['code', 'time']).agg({'bestBid1Volume': 'last', 'x': 'last', 'code': 'last', 'time': 'last', 'bestBid1': 'last', 'totalMatchVolume': 'last', 'real_MatchingVolume': 'sum'})
    if do_split_order:
        if verbosity >= 1: print(f'{Tc.CYELLOW2}do_split_order={do_split_order}{Tc.CEND}. Using accumulatedMatched{bid_or_offer} {Tc.CRED2}Warning: WORSE results{Tc.CEND}')
        df_cut = pd.merge(left=df_cut, right=split_matched_orders_by_side(),
                          how='left',
                          left_index=True, right_index=True)
        df_cut['bestBid1Volume'] += df_cut['accMatchingVolume']
        if verbosity >= 2: tok()
    else:
        if verbosity >= 1: print(f'{Tc.CYELLOW2}do_split_order={do_split_order}{Tc.CEND}. Using totalMatchVolume')
        df_cut['bestBid1Volume'] += df_cut['totalMatchVolume']
    df = None
    ################ df_pred ###############
    df_cut['weight'] = df_cut['code'].map(lambda x: Cs.DIC_WEIGHTS[x]/100)
    df_cut['fakeVolume'] = df_cut['bestBid1Volume'].groupby(level='code').diff().fillna(0)
    df_cut['value'] = df_cut['fakeVolume'] * df_cut['bestBid1']
    df_cut['pred'] = (df_cut['value'] / df_cut['weight']).map(lambda x: round(x / 1_000_000_000, 2))

    ############ filter by size ############
    df_cut.loc[df_cut['pred'] > upper_bound, 'pred'] = 0
    df_cut.loc[df_cut['pred'] < lower_bound, 'pred'] = 0
    df_cut = df_cut[df_cut['fakeVolume'] > 0]
    # print(df_cut.columns)
    df_cut = copy(df_cut[df_cut['pred'] > 0])
    # print(df_cut.columns)
    df_cut['real_MatchingVolume'] = df_cut[['real_MatchingVolume','fakeVolume']].values.min(1)
    df_cut['x'] = df_cut['x'].map(lambda x: x - x % grid_size)
    df_cut['y'] = df_cut['code'].map(lambda x: dic_stock_to_y[x])
    df_cut['t'] = df_cut['time'].map(lambda x: FRAME.indexToTime[time_int_to_x1(x)])
    df_cut['name'] = df_cut.apply(lambda x: f"{x['code']} {x['t']}" , axis=1) + df_cut['fakeVolume'].map(lambda x: f'<br>{x:,.0f}<br>') + df_cut['pred'].map(lambda x: f'({x:.2f})')

    df_cut['chart'] = 'PyLab'
    ############ filter by density ###########
    if hardcoded_threshold <= 0:
        viables = df_cut['x'].value_counts().head(head_length).index.values
        df_cut = df_cut[df_cut['x'].isin(viables)]
    else:
        df_viables = df_cut['x'].value_counts()
        viables=df_viables[df_viables >= hardcoded_threshold].index
        df_cut = df_cut[df_cut['x'].isin(viables)]


    if verbosity >= 2: tok()

    return df_cut

def load_arbit_unwind_df(df: pd.DataFrame, atype='arbit', min_lot=0.1, max_lot=200, threshold=None):
    if threshold is None: threshold = THRESHOLD
    dic = BASE_PARSER.get_ssi_id_to_stock(hardcoded=True)
    dic = {k: v for k, v in dic.items() if v in Cs.DIC_WEIGHTS}
    df = df[df['id'].isin(dic)]
    df['x'] = df['time'].map(time_int_to_x1)
    df = df[((df['time'] <= 10113500 ) & (df['time'] >= 10091545)) | ((df['time'] <= 10150000 ) & (df['time'] >= 10130045)) | (df['time']<=10091500)]

    df['code'] = df['id'].map(dic)
    if atype == 'arbit':
        df_cut = compute_df_cut(df=df,
                                head_length=0, do_split_order=True,
                                lower_bound=min_lot, upper_bound=max_lot,
                                bid_or_offer='bid',
                                grid_size=1,
                                hardcoded_threshold=threshold,
                                verbosity=0)
        df_cut['chart'] = 'Bid'
    # df_cut = filter_min_occurence(df_cut)
    # infer_most_frequent_matching_volumes(df_cut)
    if atype == 'unwind':
        df_cut = compute_df_cut(df=df,
                                 head_length=0, do_split_order=True,
                                 lower_bound=min_lot, upper_bound=max_lot,
                                 bid_or_offer='offer',
                                 grid_size=1,
                                 hardcoded_threshold=threshold,
                                 verbosity=0)
        df_cut['chart'] = 'Offer'
    return df_cut

def find_arbit_unwind(df: pd.DataFrame, atype='arbit', arbit_unwind = {}):
    def test():
        df = PADAPTERS.load_ssi_ws_hose500()
    df_cut:pd.DataFrame = load_arbit_unwind_df(df, atype)
    df_cut.reset_index(drop=True, inplace=True)
    df_cut.sort_values('x', inplace=True)
    df_cut.reset_index(drop=True, inplace=True)

    SEARCH_SEC = 25
    MIN_BB = 10
    ADD_BB_MAX_TIME = 10
    BINS1 = [0.025 * 1.25 ** x for x in range(1, 50)]
    if len(df_cut.index) == 0:
        return df_cut
    
    indices = list(range(df_cut['x'].min() + SEARCH_SEC, df_cut['x'].max(), 5))
    res = []
    #Lâu ở đây
    # print(df_cut)
    # print(indices)
    for i in indices:
        df_1:pd.DataFrame = df_cut[(df_cut['x'] > i - SEARCH_SEC) & (df_cut['x']<=i)]
        if len(df_1.index) == 0:
            continue

        df_1['timecode'] = df_1['time'].astype(str)+ df_1['code']
        df_1['volume_code'] = df_1['fakeVolume'].astype(int).astype(str) + df_1['code']
        df_1['real_volume_code'] = df_1['real_MatchingVolume'].astype(int).astype(str) + df_1['code']

        excludes = [y for x in res for y in x['timecodes']]
        df_1 = df_1[~df_1['timecode'].isin(excludes)].copy()

        if len(res) > 0:
            last_dic = res[-1]
            tmp = df_1.copy()
            tmp = tmp[~tmp['code'].isin(last_dic['codes'])].copy()
            tmp = tmp[tmp['x'] - last_dic['x'] < ADD_BB_MAX_TIME].copy()
            tmp['num_lot_diff'] = abs(tmp['pred'] - last_dic['num_lot']) / last_dic['num_lot']
            tmp = tmp[tmp['num_lot_diff'] < 0.35].sort_values('num_lot_diff').drop_duplicates('code')
            if len(tmp) > 0:
                last_dic['codes'] += tmp['code'].to_list()
                last_dic['timecodes'] += tmp['timecode'].to_list()
                last_dic['volume_codes'] += tmp['volume_code'].to_list()
                last_dic['real_volume_codes'] += tmp['real_volume_code'].to_list()
                res[-1] = last_dic
                excludes = [y for x in res for y in x['timecodes']]
                df_1 = df_1[~df_1['timecode'].isin(excludes)].copy()

        df_1[f'cut'] = pd.cut(df_1['pred'], bins=BINS1)
        df_1[f'cut'] = df_1[f'cut'].cat.codes
        counts = df_1['cut'].value_counts()
        try:
            a = counts.values[0]
        except:
            print(i)
            continue
        if (a < MIN_BB):
            # print('count < 6 skip', df['time'].values[-1], ti)
            if len(counts) < 2:
                continue
            elif counts.values[0] + counts.values[1] < MIN_BB:
                continue
        df_1 = df_1[(df_1['cut'] >= counts.index[0] - 1) & (df_1['cut'] <= counts.index[0] + 1)].copy()
        num_lot = df_1['pred'].median()
        median_ti = df_1['x'].median()

        df_1['loss_num_lot'] = (abs((df_1['pred'] - num_lot) / num_lot) + 1) ** 5
        df_1['loss_ts'] = (abs(df_1['x'] - median_ti)) ** 2 / 100
        df_1['loss'] = df_1['loss_ts'] + df_1['loss_num_lot']
        df_1 = df_1.sort_values('loss')

        n = len(df_1)
        mu = np.mean(df_1['loss'].values[:int(n * 0.8)])
        std = np.std(df_1['loss'].values[:int(n * 0.8)])

        df_1['norm_loss'] = (df_1['loss'] - mu).abs() / std
        df_1 = df_1.sort_values('norm_loss')
        df_1 = df_1.drop_duplicates('code', keep='first')
        df_1 = df_1[df_1['norm_loss'] < 6].copy()
        df_1 = df_1.sort_values('x')

        ts_rolling_err = (df_1['x'].rolling(5).mean() - df_1['x']).abs().min()

        df_1 = df_1.sort_values('pred')
        lot_rolling_err = ((df_1['pred'].rolling(5).mean() - df_1['pred']) / num_lot).abs().min()

        if ts_rolling_err / num_lot > 1:
            continue
        if lot_rolling_err > 1:
            continue

        if (len(df_1.index) >= MIN_BB + 3):
            dic = {
                'time': df_1['time'].values[0],
                'type': atype,
                'num_lot': num_lot,
                'x': int(df_1['x'].mean()),
                'codes': df_1['code'].to_list(),
                'timecodes': df_1['timecode'].to_list(),
                'volume_codes': df_1['volume_code'].to_list(),
                'real_volume_codes':df_1['real_volume_code'].to_list(),
            }
            res.append(dic)
    
    # print(df_cut)
    res2 = []
    for x in res:
        # if (x['num_lot'] >= 8) & (len(set(x['codes']).intersection(available_room_set)) >=12):
        if (x['num_lot'] >= 8) & (len(x['codes']) >= 16):
            res2.append(x)
        elif len(x['codes']) >= 16:
            res2.append(x)
    # res2 = [x for x in res if len(x['codes']) >= 16]
    # print(res2[-1])
    global arbit_dic, unwind_dic
    if atype == 'arbit':
        arbit_dic = res2
    else:
        unwind_dic = res2
    arbit_unwind[atype] = res2
    return res2

def save_raw(data:pd.DataFrame, type:str = 'arbit_raw', date = datetime.now(tz=VN_TZ).strftime("%Y_%m_%d"), required=False):
    global STORE_RAW
    if not required and STORE_RAW: return
    raw_coll = f"{date}_{type}"
    ws.drop_collection(raw_coll)
    data = data.drop(columns=['index'])
    ws.get_collection(raw_coll).insert_many(data.to_dict("records"))
    print(f"Pushed to MongoDB: {raw_coll}")
    
    STORE_RAW = True

def save_monogdb(df_arbit:pd.DataFrame, date = datetime.now(tz=VN_TZ).strftime("%Y_%m_%d"), required=False):
    global STORE_DB
    if not required and STORE_DB: return
    coll = f"{date}_arbit"
    # save to ws
    # last_timestamp = ws.get_collection(coll).find({},{'timestamp':1})
    ws.drop_collection(coll)
    ws.get_collection(coll).insert_many(df_arbit.to_dict("records"))
    # save to cmc2
    db.drop_collection(coll)
    db.get_collection(coll).insert_many(df_arbit.to_dict("records"))
    print(f"Pushed to MongoDB: {coll}")
    
    if date == datetime.now(tz=VN_TZ).strftime("%Y_%m_%d"):
        STORE_DB = True

def process_volume_codes(volume_codes):
    dic={}
    for volume_code in volume_codes:
        code = volume_code[-3:]
        volume = int(volume_code[:-3])
        dic[code] = [volume]
    return dic

def process_arbit_dic(arbit_dic, save_to: None):
    global DIC_WEIGHTS_VOLUME
    lst_df_arbit = []
    
    for idx,i in enumerate(arbit_dic):
        dic_time_codes = process_volume_codes(i['timecodes'])
        dic_volume_codes = process_volume_codes(i['volume_codes'])
        dic_real_volume_codes = process_volume_codes(i['real_volume_codes'])
        combined_dict = {key: dic_time_codes[key] + dic_volume_codes[key] + dic_real_volume_codes[key]\
            for key in dic_time_codes.keys() & dic_volume_codes.keys() & dic_real_volume_codes.keys()}
        df_tmp = pd.DataFrame.from_dict(combined_dict, orient = 'index',columns = ['time','volume','real_volume'])
        df_tmp = df_tmp.reset_index(drop=False).rename(columns={'index':'stock'})
        df_tmp['weight_volume'] = df_tmp['stock'].map(DIC_WEIGHTS_VOLUME)
        df_tmp['num_lot_stock'] = df_tmp['volume'] / df_tmp['weight_volume']
        df_tmp['num_lot_wholelot_bil'] = i['num_lot']
        df_tmp['id'] = idx
        lst_df_arbit.append(df_tmp)
    if len(lst_df_arbit) == 1:
        df_arbit = pd.DataFrame(lst_df_arbit[0])
        df_arbit['type'] = arbit_dic[0]['type']
    elif len(lst_df_arbit) >= 2:
        df_arbit = pd.concat(lst_df_arbit,ignore_index=True)
        df_arbit['type'] = arbit_dic[0]['type']
    else:
        df_arbit = pd.DataFrame()
    if len(df_arbit.index) > 0 and save_to is not None:
        db[save_to].delete_many({})
        db[save_to].insert_many(df_arbit.to_dict("records"))
        
        ###A Tai dung xoa dong nay
        if 'arbit' in save_to:
            r.set('arbit_raw_details', pickle.dumps(df_arbit), ex=28800)
        else:
            r.set('unwind_raw_details', pickle.dumps(df_arbit), ex=28800)
        ####
    
    # df_arbit1 = df_arbit.set_index(['id','stock'])
    return df_arbit   
     
# arbit_dic = {}
# unwind_dic = {}

def plot_total_arbit_unwind(df = None):
    date = datetime.now(tz=VN_TZ).strftime("%Y_%m_%d")
    # df:pd.DataFrame = load_data(date)
    if df is None: 
        # sleep(1)
        return
    # manager = Manager()
    arbit_unwind = {}
    t1 = Thread(target=find_arbit_unwind, args=(df, 'arbit', arbit_unwind), daemon=True)
    t2 = Thread(target=find_arbit_unwind, args=(df, 'unwind', arbit_unwind), daemon=True)
    t1.start()
    t2.start()
    df = None
    t1.join()
    t2.join()
    df_arbit = pd.DataFrame(columns=columns)
    df_unwind = pd.DataFrame(columns=columns)
    df_arbit.loc[0] = 0
    df_unwind.loc[0] = 0
    global arbit_dic, unwind_dic
    if 'arbit' in arbit_unwind and len(arbit_dic) > 0:

        df_arbit = pd.DataFrame(arbit_dic).sort_values(by='time').reset_index(drop=True)
        # arbit_dic = {}
        df_arbit['acc_num_lot'] = df_arbit['num_lot'].cumsum()
        t3 = Thread(target=process_arbit_dic, args=(arbit_dic, f'{date}_arbit_details',), daemon=True)
        t3.start()
        # process_arbit_dic(arbit_dic, save_to=f'{date}_arbit_details')
    df_arbit = df_arbit.reset_index(drop=False)
    if 'unwind' in arbit_unwind and len(unwind_dic) > 0:
        df_unwind = pd.DataFrame(unwind_dic).sort_values(by='time').reset_index(drop=True)
        # unwind_dic = {}
        df_unwind['acc_num_lot'] = df_unwind['num_lot'].cumsum()
        # process_arbit_dic(unwind_dic, save_to=f'{date}_unwind_details')
        t4 = Thread(target=process_arbit_dic, args=(unwind_dic, f'{date}_unwind_details',), daemon=True)
        t4.start()
    df_unwind = df_unwind.reset_index(drop=False)
    current_time = datetime.now(tz=VN_TZ)

    print('abc')
    if date == current_time.strftime("%Y_%m_%d"):
        if current_time.strftime('%X') >= '15:29:58':
            thread = Thread(target=save_raw, args=(df_arbit, 'arbit_raw', date, False, ))
            thread.start()
            thread = Thread(target=save_raw, args=(df_unwind, 'unwind_raw', date, False, ))
            thread.start()
    else:
        thread = Thread(target=save_raw, args=(df_arbit, 'arbit_raw', date, False, ))
        thread.start()
        thread = Thread(target=save_raw, args=(df_unwind, 'unwind_raw', date, False, ))
        thread.start()

    arbit_unwind_summary = {
        'lot_value': 2_500_000_000,
        'time': dt.now().timestamp(),
        'arbit': int(df_arbit['acc_num_lot'].iloc[-1] * 100)/100 if len(df_arbit.index) > 0 else 0,
        'unwind': int(df_unwind['acc_num_lot'].iloc[-1] * 100)/100 if len(df_unwind.index) > 0 else 0,
    }
    if date == datetime.now(tz=VN_TZ).strftime("%Y_%m_%d"):
        print(arbit_unwind_summary)
        # rcmc2.set(f"redis_tree.trala_be.index.arbit.ninja.tai",json.dumps(arbit_unwind_summary, default=str))
        rlv2.set(f"redis_tree.trala_be.index.arbit.ninja.tai",json.dumps(arbit_unwind_summary, default=str))
        print(f"[{datetime.now(tz=VN_TZ).strftime('%X')}] Pushed redis_tree.trala_be.index.arbit.ninja.tai")
    
    df_arbit = df_arbit[["x","time","num_lot","acc_num_lot"]]
    df_arbit = df_arbit.rename(columns={'x':"arbit_x","acc_num_lot": "Arbit", "num_lot":"arbit_raw"})
    df_unwind = df_unwind[["x","time","num_lot","acc_num_lot"]]  
    df_unwind = df_unwind.rename(columns={'x':"unwind_x","acc_num_lot": "Unwind", "num_lot":"unwind_raw"})

    df_arbit = df_arbit.merge(df_unwind, how="outer")
    df_unwind = None
    df_arbit = df_arbit[df_arbit['time'] > 0].reset_index(drop=True)
    df_arbit['time'] =  df_arbit['time'].map(lambda x: FRAME.indexToTime[time_int_to_x1(x)])
    df_arbit['datetime'] = pd.to_datetime(date + df_arbit['time'], format='%Y_%m_%d%H:%M:%S').map(lambda x: x.replace(tzinfo=VN_TZ))
    if date == datetime.now(tz=VN_TZ).strftime("%Y_%m_%d"):
        current_time = pd.to_datetime(datetime.now(tz=VN_TZ).strftime("%Y_%m_%d%H:%M:%S"), format="%Y_%m_%d%H:%M:%S").replace(tzinfo=VN_TZ)
    else:
        current_time = pd.to_datetime(f"{date}15:30:00", format="%Y_%m_%d%H:%M:%S").replace(tzinfo=VN_TZ)

    found = df_arbit[df_arbit['datetime'] == current_time]
    if len(found.index) == 0:
        df_arbit.loc[len(df_arbit.index)] = [None,None,None,None,None,None,None,current_time]
    df_arbit = df_arbit.set_index('datetime').resample('1S').last().fillna(method='ffill').reset_index()
    df_arbit = df_arbit.fillna(0)
    df_arbit['time'] = df_arbit['datetime'].dt.strftime('%X')
    df_arbit['timestamp'] = df_arbit['datetime'].view(np.int64) / 1e9
    
    ###A Tai dung xoa dong nay
    r.set('arbit_unwind_v4_backend', pickle.dumps(df_arbit), ex=28800)
    ####
    if date == datetime.now(tz=VN_TZ).strftime("%Y_%m_%d"):
        current_time = datetime.now(tz=VN_TZ).strftime('%X')
        if '08:45:00' <= current_time <= '15:03:00':
            df_arbit.drop(columns=['arbit_x','unwind_x','datetime',"arbit_raw","unwind_raw"], inplace=True)
            df_arbit = df_arbit.set_index('timestamp')
            df_arbit.index = pd.to_datetime(df_arbit.index, unit='s')
            df_arbit = df_arbit.resample('15s').last()
            df_arbit['timestamp'] = df_arbit.index.astype(int)/1e9
            data = json.dumps(df_arbit.to_dict('list'), default=str)
            # print(data)
            # rcmc2.set("arbit_unwind_v4_backend", data)
            rlv2.set("arbit_unwind_v4_backend", data)
            print(f"[{current_time}] Pushed arbit_unwind_v4_backend")
            store_data = False
        else:
            store_data = True
    else:
        store_data = True
    if store_data:
        # store mongodb here
        df_arbit.drop(columns=['datetime'], inplace=True)
        thread = Thread(target=save_monogdb, args=(df_arbit, date, False, ))
        thread.start()
    df_arbit = None

def start_new_day(date = None):   
    print("Start New Day") 
    global dic_stock_to_y, CURRENT_DATE, STORE_DB, STORE_RAW, available_room_set, DIC_WEIGHTS_VOLUME
    if date is None:
        date = datetime.now(tz=VN_TZ).strftime("%Y_%m_%d")

    # rcmc2.delete('arbit_unwind_v4_backend', 'redis_tree.trala_be.index.arbit.ninja.tai')
    rlv2.delete('arbit_unwind_v4_backend','redis_tree.trala_be.index.arbit.ninja.tai')
    vn30_weight = load_vn30_weights_from_db(date)
    # vn30_weight = pd.read_excel('/home/ubuntu/Tai/Production/v2/VN30_basket.xlsx').rename(columns={
    #     'Stock':'stock',
    #     'Volume':'volume',
    #     'Weighting':'weights'
    # })
    # vn30_weight['weights'] = vn30_weight['weights'] * 100
    vn30_weight['day'] = date
    w_df = vn30_weight[vn30_weight['day'] == date]
    w0 = dict(zip(w_df['stock'],w_df['weights']))
    Cs.DIC_WEIGHTS = {k: w0[k] for k in w0.keys()}
    
    w0_volume = dict(zip(w_df['stock'],w_df['volume']))
    DIC_WEIGHTS_VOLUME = {k: w0_volume[k] for k in w0_volume.keys()}

    dic_stock_to_y = {k: v for k, v in zip(Cs.DIC_WEIGHTS.keys(), range(len(Cs.DIC_WEIGHTS)))}
    # available_room_set = set(w_df[w_df['is_out_of_room'] == 0]['stock'])
    CURRENT_DATE = date
    STORE_DB = False
    STORE_RAW = False

############################################ VARS ############################################

CURRENT_DATE = None
STORE_DB = None
STORE_RAW = None
DIC_WEIGHTS_VOLUME = None

pd.set_option('display.max_columns', 200)
pd.set_option('display.width', 300)


columns = ['timestamp','time', 'type', 'num_lot', 'x', 'codes', 'timecodes', 'acc_num_lot']
tick_times = [':'.join(x) + ':00' for x in
              itertools.product(['09', '10', '11', '12', '13', '14'], ['00', '15', '30', '45'])]
tick_times = [x for x in tick_times if x in FRAME.timeToIndex if x not in ['13:00:00', '09:00:00', '14:45:00']]
tick_vals = mmap(lambda x: FRAME.timeToIndex[x], tick_times)

# _, _, psave, pload = gen_plasma_functions(db=0)
# rlv2 = secured_redis_connector(host='lv2', decode_responses=True)

THRESHOLD = 2
MIN_LOT = 0.1
MAX_LOT = 50
INITIATED = True
# PADAPTERS.maybe_initiate()

# if __name__=="__main__":
#     while True:        
#         main()
#         sleep(1)


def main(hose500 = None):
    current_time = datetime.now(tz=VN_TZ)
    date = current_time.strftime("%Y_%m_%d")
    dayOfWeek = current_time.weekday()
    current_time = current_time.strftime('%X') 
    try:
        if CURRENT_DATE != date:
            start_new_day()
    except Exception as ex:
        print(str(ex))
    if dayOfWeek <= 4 and ('09:00:00' <= current_time < '12:35:00' or '12:46:00' <= current_time <= '15:05:00'):        
        plot_total_arbit_unwind(hose500)
    else:
        print(f"[{current_time}] Ngoai gio giao dich")

