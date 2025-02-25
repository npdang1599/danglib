"""
Hose500 columns:

'session', 'time', 'code', 'matchedBy', 'last', 
'matchingVolume', 'totalMatchVolume', 'totalMatchValue', 
'foreignerBuyVolume', 'foreignerSellVolume', 'matchingValue', 
'timestamp', 'refPrice', 'bestBid1', 'bestBid1Volume', 'bestBid2', 
'bestBid2Volume', 'bestBid3', 'bestBid3Volume', 'bestOffer1', 
'bestOffer1Volume', 'bestOffer2', 'bestOffer2Volume', 
'bestOffer3', 'bestOffer3Volume', 'id', 'sequence', 'stock'
"""

"""
Ninja Backend System v2
-----------------------
Hệ thống phân tích thị trường chứng khoán VN để phát hiện cơ hội arbitrage và unwind
"""

import os
import json
import pandas as pd
import numpy as np
import warnings
import redis
import pickle
import time
from copy import copy
from threading import Thread
from datetime import datetime as dt
from dateutil import tz
from pymongo import MongoClient
from multiprocessing import Process, Manager

# Suppressing warnings
warnings.filterwarnings('ignore')

# Importing custom modules
from dc_server.lazy_core import gen_plasma_functions
from dc_server.sotola import FRAME
from dc_server.ssi_ws.ssi_ws_stock_parser import BASE_PARSER, PARSER
from danglib.lazy_core import Cs
from danglib.utils import check_run_with_interactive

# Generating plasma functions
_, _, _, pload = gen_plasma_functions(5)

# Constants
VN_TZ = tz.gettz("Asia/Ho_Chi_Minh")
THRESHOLD = 2
MIN_LOT = 0.1
MAX_LOT = 50

# Database connections
db = MongoClient(
    host="cmc", 
    port=27022, 
    username='admin', 
    password=os.getenv("CMC_MONGO_PASSWORD"), 
    authSource='admin'
)["curated"]

ws = MongoClient('ws', 27021)["curated"]

# Redis connections
redis_ws = redis.StrictRedis(host="ws", port=6379)
redis_cmc2 = redis.StrictRedis(host="cmc2", port=6379)
redis_lv2 = redis.StrictRedis(host="lv2", port=6379)

# Global variables
columns = ['timestamp', 'time', 'type', 'num_lot', 'x', 'codes', 'timecodes', 'acc_num_lot']
CURRENT_DATE = "2025_02_20"
STORE_DB = None
STORE_RAW = None
DIC_WEIGHTS_VOLUME = None
arbit_dic = []
unwind_dic = []

# Settings for pandas display
pd.set_option('display.max_columns', 200)
pd.set_option('display.width', 300)

class PADAPTERS:
    """Adapter class for loading data from various sources"""
    
    @staticmethod
    def load_ssi_ws_hose500():
        path = '/data/dang/test_hose500_data.pickle'
        df: pd.DataFrame = pd.read_pickle(path)
        return df


def time_int_to_x1(t):
    """
    Converts time integer to frame index
    
    Args:
        t: Time integer
        
    Returns:
        int: Frame index for the given time
    """
    t = str(t)
    t = t[2:4] + ':' + t[4:6] + ':' + t[6:8]
    
    # Handle special time periods
    if '11:30:59' < t < '13:00:00':
        t = '11:30:59'
    elif t > '14:45:59':
        t = '14:45:59'
    
    return FRAME.timeToIndex[t]


def load_vn30_weights_from_db(day=None):
    """
    Load VN30 index weights from database
    
    Args:
        day (str, optional): Date string in format YYYY_MM_DD
        
    Returns:
        pd.DataFrame: DataFrame with VN30 weights
    """
    weights_db = MongoClient('ws', 27021)['weights']
    coll = weights_db['vn30_weights']
    
    if day is None:
        day = dt.now(tz=VN_TZ).strftime("%Y_%m_%d")
    
    # Try to find weights for current day or earlier
    df = list(coll.find({'day': {'$lte': day}}, {'_id': 0}).sort('day', -1).limit(30))
    
    # If no data found, get future weights
    if len(df) == 0:
        df = list(coll.find({'day': {'$gte': day}}, {'_id': 0}).sort('day', 1).limit(30))
    
    df = pd.DataFrame(df)
    return df


def compute_df_cut(df=None, head_length=100, upper_bound=12.0, lower_bound=8.0, 
                  bid_or_offer='bid', grid_size=5, verbosity=1, do_split_order=False, 
                  hardcoded_threshold=0):
    """
    Process and filter market data to find potential arbitrage patterns
    
    Args:
        df (pd.DataFrame, optional): Market data
        head_length (int): Maximum number of data points to consider
        upper_bound (float): Upper limit for prediction values
        lower_bound (float): Lower limit for prediction values
        bid_or_offer (str): Type of orders to analyze ('bid' or 'offer')
        grid_size (int): Grid size for visualization
        verbosity (int): Level of output verbosity
        do_split_order (bool): Whether to split orders by side
        hardcoded_threshold (int): Fixed threshold for filtering

    Returns:
        pd.DataFrame: Processed and filtered data
    """
    def test():
        df=None
        head_length=100
        upper_bound=12.0
        lower_bound=8.0
        
        bid_or_offer='bid'
        grid_size=5
        verbosity=1
        do_split_order=False
        
        hardcoded_threshold=0

    # Capitalize first letter for consistent lookup
    bid_or_offer = bid_or_offer.lower().capitalize()
    lookup = {'Bid': 1, 'Offer': -1}
    
    # Log timing if verbose
    if verbosity >= 2: 
        print("Starting data processing...")
    
    # Load data if not provided
    if df is None: 
        df = PADAPTERS.load_ssi_ws_hose500()
    
    if len(df.index) <= 0:
        return df

    # Helper function for order splitting
    def split_matched_orders_by_side():
        """Split matched orders by bid/offer side"""
        # Chỉ đơn giản là tính accumulate matching volume theo order side
        df_matched = df.groupby(['matchedBy', 'code', 'time']).agg({'matchingVolume': sum}).loc[lookup[bid_or_offer]]
        df_matched['matchedVolume'] = df_matched.groupby(level='code').agg({'matchingVolume': 'cumsum'})
        df_matched_lookup = df_matched.reset_index().pivot(
            index='time', columns='code', values='matchedVolume'
        ).fillna(method='ffill').fillna(0)
        return df_matched_lookup.unstack().to_frame().rename(columns={0: 'accMatchingVolume'})

    # Process data for cutting
    df_cut = copy(df)
    df_cut = df_cut.sort_values(['code', 'totalMatchVolume'])
    
    # Calculate real matching volume
    df_cut['real_MatchingVolume'] = df_cut['totalMatchVolume'].diff()
    df_cut['real_MatchingVolume'] = np.where(df_cut['real_MatchingVolume'] < 0, 0, df_cut['real_MatchingVolume'])
    df_cut['real_MatchingVolume'].iloc[0] = 0
    
    # Adjust for bid/offer type
    if bid_or_offer != 'Bid':
        df_cut[['bestBid1', 'bestBid1Volume']] = df_cut[['bestOffer1', 'bestOffer1Volume']].copy()
    
    df_cut['bestBid1'] = df_cut['refPrice']
    
    # Group and aggregate data
    df_cut = df_cut.groupby(['code', 'time']).agg({
        'bestBid1Volume': 'last', 
        'x': 'last', 
        'code': 'last', 
        'time': 'last', 
        'bestBid1': 'last', 
        'totalMatchVolume': 'last', 
        'real_MatchingVolume': 'sum'
    })
    
    # Apply order splitting if requested
    if do_split_order:
        if verbosity >= 1:
            print(f"Using split orders for {bid_or_offer} side")
        
        df_cut = pd.merge(
            left=df_cut, 
            right=split_matched_orders_by_side(),
            how='left',
            left_index=True, 
            right_index=True
        )
        df_cut['bestBid1Volume'] += df_cut['accMatchingVolume']
    else:
        if verbosity >= 1:
            print(f"Using totalMatchVolume without splitting")
        
        df_cut['bestBid1Volume'] += df_cut['totalMatchVolume']
    
    # Clean up memory
    df = None
    
    # Add prediction features
    df_cut['weight'] = df_cut['code'].map(lambda x: Cs.DIC_WEIGHTS[x] / 100)
    df_cut['fakeVolume'] = df_cut['bestBid1Volume'].groupby(level='code').diff().fillna(0)
    df_cut['value'] = df_cut['fakeVolume'] * df_cut['bestBid1']
    df_cut['pred'] = (df_cut['value'] / df_cut['weight']).map(lambda x: round(x / 1_000_000_000, 2))

    # Filter by size bounds
    df_cut.loc[df_cut['pred'] > upper_bound, 'pred'] = 0
    df_cut.loc[df_cut['pred'] < lower_bound, 'pred'] = 0
    df_cut = df_cut[df_cut['fakeVolume'] > 0]
    df_cut = copy(df_cut[df_cut['pred'] > 0])
    
    # Further processing for visualization
    df_cut['real_MatchingVolume'] = df_cut[['real_MatchingVolume', 'fakeVolume']].values.min(1)
    df_cut['x'] = df_cut['x'].map(lambda x: x - x % grid_size)
    df_cut['y'] = df_cut['code'].map(lambda x: dic_stock_to_y[x])
    df_cut['t'] = df_cut['time'].map(lambda x: FRAME.indexToTime[time_int_to_x1(x)])
    df_cut['name'] = (
        df_cut.apply(lambda x: f"{x['code']} {x['t']}", axis=1) + 
        df_cut['fakeVolume'].map(lambda x: f'<br>{x:,.0f}<br>') + 
        df_cut['pred'].map(lambda x: f'({x:.2f})')
    )
    df_cut['chart'] = 'PyLab'
    
    # Filter by density
    if hardcoded_threshold <= 0:
        viables = df_cut['x'].value_counts().head(head_length).index.values
        df_cut = df_cut[df_cut['x'].isin(viables)]
    else:
        df_viables = df_cut['x'].value_counts()
        viables = df_viables[df_viables >= hardcoded_threshold].index
        df_cut = df_cut[df_cut['x'].isin(viables)]

    return df_cut


def load_arbit_unwind_df(df, atype='arbit', min_lot=0.1, max_lot=200, threshold=None):
    """
    Load and prepare data for arbitrage or unwind analysis
    
    Args:
        df (pd.DataFrame): Market data
        atype (str): Analysis type ('arbit' or 'unwind')
        min_lot (float): Minimum lot size to consider
        max_lot (float): Maximum lot size to consider
        threshold (int, optional): Threshold for filtering
        
    Returns:
        pd.DataFrame: Prepared data for analysis
    """
    if threshold is None: 
        threshold = THRESHOLD
    
    # Filter stocks with weights
    dic = BASE_PARSER.get_ssi_id_to_stock(hardcoded=True)
    dic = {k: v for k, v in dic.items() if v in Cs.DIC_WEIGHTS}
    df = df[df['id'].isin(dic)]
    
    # Convert time to frame index
    df['x'] = df['time'].map(time_int_to_x1)
    
    # Filter by specific time periods
    df = df[
        ((df['time'] <= 10113500) & (df['time'] >= 10091545)) | 
        ((df['time'] <= 10150000) & (df['time'] >= 10130045)) | 
        (df['time'] <= 10091500)
    ]

    # Map codes
    df['code'] = df['id'].map(dic)
    
    # Process based on analysis type
    if atype == 'arbit':
        df_cut = compute_df_cut(
            df=df,
            head_length=0, 
            do_split_order=True,
            lower_bound=min_lot, 
            upper_bound=max_lot,
            bid_or_offer='bid',
            grid_size=1,
            hardcoded_threshold=threshold,
            verbosity=0
        )
        df_cut['chart'] = 'Bid'
    elif atype == 'unwind':
        df_cut = compute_df_cut(
            df=df,
            head_length=0, 
            do_split_order=True,
            lower_bound=min_lot, 
            upper_bound=max_lot,
            bid_or_offer='offer',
            grid_size=1,
            hardcoded_threshold=threshold,
            verbosity=0
        )
        df_cut['chart'] = 'Offer'
    
    return df_cut


def find_arbit_unwind(df, atype='arbit', arbit_unwind={}):
    """
    Find arbitrage or unwind opportunities in market data
    
    Args:
        df (pd.DataFrame): Market data
        atype (str): Analysis type ('arbit' or 'unwind')
        arbit_unwind (dict): Container for results
        
    Returns:
        list: List of detected opportunities
    """
    # Process data for given type
    df_cut = load_arbit_unwind_df(df, atype)
    df_cut.reset_index(drop=True, inplace=True)
    df_cut.sort_values('x', inplace=True)
    df_cut.reset_index(drop=True, inplace=True)

    # Constants for searching
    SEARCH_SEC = 25
    MIN_BB = 10
    ADD_BB_MAX_TIME = 10
    BINS1 = [0.025 * 1.25 ** x for x in range(1, 50)]
    
    if len(df_cut.index) == 0:
        return []
    
    # Generate search indices
    indices = list(range(df_cut['x'].min() + SEARCH_SEC, df_cut['x'].max(), 5))
    res = []
    
    # Search for patterns
    for i in indices:
        # Get data within time window
        df_1 = df_cut[(df_cut['x'] > i - SEARCH_SEC) & (df_cut['x'] <= i)]
        if len(df_1.index) == 0:
            continue

        # Create composite keys for tracking
        df_1['timecode'] = df_1['time'].astype(str) + df_1['code']
        df_1['volume_code'] = df_1['fakeVolume'].astype(int).astype(str) + df_1['code']
        df_1['real_volume_code'] = df_1['real_MatchingVolume'].astype(int).astype(str) + df_1['code']

        # Exclude already processed entries
        excludes = [y for x in res for y in x['timecodes']]
        df_1 = df_1[~df_1['timecode'].isin(excludes)].copy()

        # Try to extend the last result if possible
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

        # Group predictions into bins
        df_1['cut'] = pd.cut(df_1['pred'], bins=BINS1)
        df_1['cut'] = df_1['cut'].cat.codes
        counts = df_1['cut'].value_counts()
        
        # Check if enough samples in the most common bin
        try:
            a = counts.values[0]
        except:
            print(f"No counts at index {i}")
            continue
            
        if (a < MIN_BB):
            if len(counts) < 2:
                continue
            elif counts.values[0] + counts.values[1] < MIN_BB:
                continue
                
        # Filter entries within relevant bins
        df_1 = df_1[(df_1['cut'] >= counts.index[0] - 1) & (df_1['cut'] <= counts.index[0] + 1)].copy()
        num_lot = df_1['pred'].median()
        median_ti = df_1['x'].median()

        # Calculate loss metrics
        df_1['loss_num_lot'] = (abs((df_1['pred'] - num_lot) / num_lot) + 1) ** 5
        df_1['loss_ts'] = (abs(df_1['x'] - median_ti)) ** 2 / 100
        df_1['loss'] = df_1['loss_ts'] + df_1['loss_num_lot']
        df_1 = df_1.sort_values('loss')
        
        # Normalize loss
        n = len(df_1)
        mu = np.mean(df_1['loss'].values[:int(n * 0.8)])
        std = np.std(df_1['loss'].values[:int(n * 0.8)])
        df_1['norm_loss'] = (df_1['loss'] - mu).abs() / std
        
        # Filter and sort results
        df_1 = df_1.sort_values('norm_loss')
        df_1 = df_1.drop_duplicates('code', keep='first')
        df_1 = df_1[df_1['norm_loss'] < 6].copy()

        # Check time series consistency
        df_1 = df_1.sort_values('x')
        ts_rolling_err = (df_1['x'].rolling(5).mean() - df_1['x']).abs().min()

        # Check lot size consistency
        df_1 = df_1.sort_values('pred')
        lot_rolling_err = ((df_1['pred'].rolling(5).mean() - df_1['pred']) / num_lot).abs().min()

        # Skip if consistency checks fail
        if ts_rolling_err / num_lot > 1 or lot_rolling_err > 1:
            continue
            
        # Create result if enough samples found
        if (len(df_1.index) >= MIN_BB + 3):
            dic = {
                'time': df_1['time'].values[0],
                'type': atype,
                'num_lot': num_lot,
                'x': int(df_1['x'].mean()),
                'codes': df_1['code'].to_list(),
                'timecodes': df_1['timecode'].to_list(),
                'volume_codes': df_1['volume_code'].to_list(),
                'real_volume_codes': df_1['real_volume_code'].to_list(),
            }
            res.append(dic)
    
    # Filter results based on criteria
    res2 = []
    for x in res:
        if (x['num_lot'] >= 8 and len(x['codes']) >= 16) or len(x['codes']) >= 16:
            res2.append(x)
    
    # Update global variables
    global arbit_dic, unwind_dic
    if atype == 'arbit':
        arbit_dic = res2
    else:
        unwind_dic = res2
        
    # Store results
    arbit_unwind[atype] = res2
    return res2


def save_raw(data, type='arbit_raw', date=None, required=False):
    """
    Save raw data to MongoDB
    
    Args:
        data (pd.DataFrame): Data to save
        type (str): Data type
        date (str, optional): Date string
        required (bool): Whether saving is required
    """
    global STORE_RAW
    
    if not required and STORE_RAW: 
        return
        
    if date is None:
        date = dt.now(tz=VN_TZ).strftime("%Y_%m_%d")
        
    # Create collection name and save data
    raw_coll = f"{date}_{type}"
    ws.drop_collection(raw_coll)
    
    if 'index' in data.columns:
        data = data.drop(columns=['index'])
        
    ws.get_collection(raw_coll).insert_many(data.to_dict("records"))
    print(f"Pushed to MongoDB: {raw_coll}")
    
    STORE_RAW = True


def save_monogdb(df_arbit, date=None, required=False):
    """
    Save processed data to MongoDB
    
    Args:
        df_arbit (pd.DataFrame): Data to save
        date (str, optional): Date string
        required (bool): Whether saving is required
    """
    global STORE_DB
    
    if not required and STORE_DB: 
        return
        
    if date is None:
        date = dt.now(tz=VN_TZ).strftime("%Y_%m_%d")
        
    # Collection name
    coll = f"{date}_arbit"
    
    # Save to ws database
    ws.drop_collection(coll)
    ws.get_collection(coll).insert_many(df_arbit.to_dict("records"))
    
    # Save to cmc2 database
    db.drop_collection(coll)
    db.get_collection(coll).insert_many(df_arbit.to_dict("records"))
    
    print(f"Pushed to MongoDB: {coll}")
    
    # Mark as stored if for current date
    if date == dt.now(tz=VN_TZ).strftime("%Y_%m_%d"):
        STORE_DB = True


def process_volume_codes(volume_codes):
    """
    Process volume codes into a dictionary
    
    Args:
        volume_codes (list): List of volume codes
        
    Returns:
        dict: Dictionary mapping codes to volumes
    """
    dic = {}
    for volume_code in volume_codes:
        code = volume_code[-3:]
        volume = int(volume_code[:-3])
        dic[code] = [volume]
    return dic


def process_arbit_dic(arbit_dic, save_to=None):
    """
    Process arbitrage dictionary into DataFrame format
    
    Args:
        arbit_dic (list): List of arbitrage opportunities
        save_to (str, optional): Collection to save results
        
    Returns:
        pd.DataFrame: Processed data
    """
    global DIC_WEIGHTS_VOLUME
    lst_df_arbit = []
    
    # Process each opportunity
    for idx, i in enumerate(arbit_dic):
        # Extract data from codes
        dic_time_codes = process_volume_codes(i['timecodes'])
        dic_volume_codes = process_volume_codes(i['volume_codes'])
        dic_real_volume_codes = process_volume_codes(i['real_volume_codes'])
        
        # Combine dictionaries
        combined_dict = {
            key: dic_time_codes[key] + dic_volume_codes[key] + dic_real_volume_codes[key]
            for key in dic_time_codes.keys() & dic_volume_codes.keys() & dic_real_volume_codes.keys()
        }
        
        # Create DataFrame
        df_tmp = pd.DataFrame.from_dict(
            combined_dict, 
            orient='index',
            columns=['time', 'volume', 'real_volume']
        )
        df_tmp = df_tmp.reset_index(drop=False).rename(columns={'index': 'stock'})
        
        # Add additional data
        df_tmp['weight_volume'] = df_tmp['stock'].map(DIC_WEIGHTS_VOLUME)
        df_tmp['num_lot_stock'] = df_tmp['volume'] / df_tmp['weight_volume']
        df_tmp['num_lot_wholelot_bil'] = i['num_lot']
        df_tmp['id'] = idx
        lst_df_arbit.append(df_tmp)
    
    # Combine results
    if len(lst_df_arbit) == 1:
        df_arbit = pd.DataFrame(lst_df_arbit[0])
        df_arbit['type'] = arbit_dic[0]['type']
    elif len(lst_df_arbit) >= 2:
        df_arbit = pd.concat(lst_df_arbit, ignore_index=True)
        df_arbit['type'] = arbit_dic[0]['type']
    else:
        df_arbit = pd.DataFrame()
    
    # Save to database if requested
    if len(df_arbit.index) > 0 and save_to is not None:
        db[save_to].delete_many({})
        db[save_to].insert_many(df_arbit.to_dict("records"))
        
        # Cache in Redis
        if 'arbit' in save_to:
            redis_ws.set('arbit_raw_details', pickle.dumps(df_arbit), ex=28800)
        else:
            redis_ws.set('unwind_raw_details', pickle.dumps(df_arbit), ex=28800)
    
    return df_arbit


def plot_total_arbit_unwind(df=None):
    """
    Process, analyze and visualize arbitrage and unwind data - Synchronous version
    
    Args:
        df (pd.DataFrame, optional): Market data
    """
    date = dt.now(tz=VN_TZ).strftime("%Y_%m_%d")
    
    if df is None:
        return
    
    # Initialize dictionaries to store results
    arbit_results = {}
    unwind_results = {}
    
    # Find arbitrage and unwind opportunities sequentially
    arbit_list = find_arbit_unwind(df.copy(), 'arbit', arbit_results)
    unwind_list = find_arbit_unwind(df.copy(), 'unwind', unwind_results)
    
    # Release memory
    df = None
    
    # Initialize empty dataframes
    df_arbit = pd.DataFrame(columns=columns)
    df_unwind = pd.DataFrame(columns=columns)
    df_arbit.loc[0] = 0
    df_unwind.loc[0] = 0
    
    global arbit_dic, unwind_dic
    
    # Process arbitrage data
    if arbit_list and len(arbit_list) > 0:
        # Create DataFrame from arbitrage results
        df_arbit = pd.DataFrame(arbit_list).sort_values(by='time').reset_index(drop=True)
        df_arbit['acc_num_lot'] = df_arbit['num_lot'].cumsum()
        
        # Process details
        arbit_details = process_arbit_dic(arbit_list, f'{date}_arbit_details')
    
    df_arbit = df_arbit.reset_index(drop=False)
    
    # Process unwind data
    if unwind_list and len(unwind_list) > 0:
        # Create DataFrame from unwind results
        df_unwind = pd.DataFrame(unwind_list).sort_values(by='time').reset_index(drop=True)
        df_unwind['acc_num_lot'] = df_unwind['num_lot'].cumsum()
        
        # Process details
        unwind_details = process_arbit_dic(unwind_list, f'{date}_unwind_details')
    
    df_unwind = df_unwind.reset_index(drop=False)
    current_time = dt.now(tz=VN_TZ)

    # Save raw data at end of day or if not current day
    if date == current_time.strftime("%Y_%m_%d"):
        if current_time.strftime('%X') >= '15:29:58':
            save_raw(df_arbit, 'arbit_raw', date, False)
            save_raw(df_unwind, 'unwind_raw', date, False)
    else:
        save_raw(df_arbit, 'arbit_raw', date, False)
        save_raw(df_unwind, 'unwind_raw', date, False)

    # Create summary
    # Create summary of trading activity
    arbit_unwind_summary = {
        'lot_value': 2_500_000_000,
        'time': dt.now().timestamp(),
        'arbit': int(df_arbit['acc_num_lot'].iloc[-1] * 100)/100 if len(df_arbit.index) > 0 else 0,
        'unwind': int(df_unwind['acc_num_lot'].iloc[-1] * 100)/100 if len(df_unwind.index) > 0 else 0,
    }
    
    # Update Redis with summary information
    if date == dt.now(tz=VN_TZ).strftime("%Y_%m_%d"):
        print(arbit_unwind_summary)
        redis_lv2.set(
            f"redis_tree.trala_be.index.arbit.ninja.tai",
            json.dumps(arbit_unwind_summary, default=str)
        )
        print(f"[{dt.now(tz=VN_TZ).strftime('%X')}] Pushed redis_tree.trala_be.index.arbit.ninja.tai")
    
    # Prepare combined data for visualization
    df_arbit = df_arbit[["x", "time", "num_lot", "acc_num_lot"]]
    df_arbit = df_arbit.rename(columns={
        'x': "arbit_x",
        'acc_num_lot': "Arbit", 
        "num_lot": "arbit_raw"
    })
    
    df_unwind = df_unwind[["x", "time", "num_lot", "acc_num_lot"]]  
    df_unwind = df_unwind.rename(columns={
        'x': "unwind_x",
        'acc_num_lot': "Unwind", 
        "num_lot": "unwind_raw"
    })

    # Merge data and clean up
    df_arbit = df_arbit.merge(df_unwind, how="outer")
    df_unwind = None  # Release memory
    df_arbit = df_arbit[df_arbit['time'] > 0].reset_index(drop=True)
    
    # Format time
    df_arbit['time'] = df_arbit['time'].map(lambda x: FRAME.indexToTime[time_int_to_x1(x)])
    df_arbit['datetime'] = pd.to_datetime(
        date + df_arbit['time'], 
        format='%Y_%m_%d%H:%M:%S'
    ).map(lambda x: x.replace(tzinfo=VN_TZ))
    
    # Set current time based on date
    if date == dt.now(tz=VN_TZ).strftime("%Y_%m_%d"):
        current_time = pd.to_datetime(
            dt.now(tz=VN_TZ).strftime("%Y_%m_%d%H:%M:%S"), 
            format="%Y_%m_%d%H:%M:%S"
        ).replace(tzinfo=VN_TZ)
    else:
        current_time = pd.to_datetime(
            f"{date}15:30:00", 
            format="%Y_%m_%d%H:%M:%S"
        ).replace(tzinfo=VN_TZ)

    # Add current time to data if not present
    found = df_arbit[df_arbit['datetime'] == current_time]
    if len(found.index) == 0:
        df_arbit.loc[len(df_arbit.index)] = [None, None, None, None, None, None, None, current_time]
    
    # Resample to ensure regular time intervals
    df_arbit = (
        df_arbit.set_index('datetime')
        .resample('1S')
        .last()
        .fillna(method='ffill')
        .reset_index()
    )
    
    df_arbit = df_arbit.fillna(0)
    df_arbit['time'] = df_arbit['datetime'].dt.strftime('%X')
    df_arbit['timestamp'] = df_arbit['datetime'].view(np.int64) / 1e9
    
    # Cache for quick access
    redis_ws.set('arbit_unwind_v4_backend', pickle.dumps(df_arbit), ex=28800)
    
    # If current day and within trading hours, update Redis
    store_data = True
    if date == dt.now(tz=VN_TZ).strftime("%Y_%m_%d"):
        current_time = dt.now(tz=VN_TZ).strftime('%X')
        if '08:45:00' <= current_time <= '15:03:00':
            # Prepare data for frontend
            df_frontend = df_arbit.copy()
            df_frontend.drop(columns=[
                'arbit_x', 'unwind_x', 'datetime', "arbit_raw", "unwind_raw"
            ], inplace=True)
            
            df_frontend = df_frontend.set_index('timestamp')
            df_frontend.index = pd.to_datetime(df_frontend.index, unit='s')
            df_frontend = df_frontend.resample('15s').last()
            df_frontend['timestamp'] = df_frontend.index.astype(int)/1e9
            
            # Update Redis
            data = json.dumps(df_frontend.to_dict('list'), default=str)
            redis_lv2.set("arbit_unwind_v4_backend", data)
            print(f"[{current_time}] Pushed arbit_unwind_v4_backend")
            store_data = False
            
            # Clean up frontend data
            df_frontend = None
    
    # Store to MongoDB if needed
    if store_data:
        df_mongodb = df_arbit.copy()
        df_mongodb.drop(columns=['datetime'], inplace=True)
        save_monogdb(df_mongodb, date, False)
        df_mongodb = None
    
    # Clean up
    df_arbit = None



def start_new_day(date=None):
    """
    Initialize system for a new trading day
    
    Args:
        date (str, optional): Date string in format YYYY_MM_DD
    """   
    print("Starting New Trading Day") 
    global dic_stock_to_y, CURRENT_DATE, STORE_DB, STORE_RAW, DIC_WEIGHTS_VOLUME
    
    if date is None:
        date = dt.now(tz=VN_TZ).strftime("%Y_%m_%d")

    # Clear Redis cache
    redis_lv2.delete('arbit_unwind_v4_backend', 'redis_tree.trala_be.index.arbit.ninja.tai')
    
    # Load VN30 weights
    vn30_weight = load_vn30_weights_from_db(date)
    vn30_weight['day'] = date
    w_df = vn30_weight[vn30_weight['day'] == date]
    
    # Create weights dictionary
    w0 = dict(zip(w_df['stock'], w_df['weights']))
    Cs.DIC_WEIGHTS = {k: w0[k] for k in w0.keys()}
    
    # Create volume weights dictionary
    w0_volume = dict(zip(w_df['stock'], w_df['volume']))
    DIC_WEIGHTS_VOLUME = {k: w0_volume[k] for k in w0_volume.keys()}

    # Create stock to y-position mapping
    dic_stock_to_y = {k: v for k, v in zip(Cs.DIC_WEIGHTS.keys(), range(len(Cs.DIC_WEIGHTS)))}
    
    # Update globals
    CURRENT_DATE = date
    STORE_DB = False
    STORE_RAW = False


def main(hose500=None):
    """
    Main function to analyze market data
    
    Args:
        hose500 (pd.DataFrame, optional): Pre-loaded market data
    """
    # Get current time and check trading hours
    current_time = dt.now(tz=VN_TZ)
    date = current_time.strftime("%Y_%m_%d")
    dayOfWeek = current_time.weekday()
    current_time_str = current_time.strftime('%X') 
    
    # Initialize new day if needed
    try:
        if CURRENT_DATE != date:
            start_new_day(CURRENT_DATE)
    except Exception as ex:
        print(f"Error starting new day: {str(ex)}")
    
    # Only run during trading hours
    is_trading_hours = (
        dayOfWeek <= 4 and 
        ('09:00:00' <= current_time_str < '12:35:00' or '12:46:00' <= current_time_str <= '15:05:00')
    )
    
    if is_trading_hours:        
        plot_total_arbit_unwind(hose500)
    else:
        print(f"[{current_time_str}] Outside trading hours")


# If script is run directly
if __name__ == "__main__" and not check_run_with_interactive():
    # while True:
    #     main()
    #     time.sleep(1)  # Sleep for 1 second before next iteration
    print('hello')