from danglib.pslab.resources import Adapters, Globs
from danglib.pslab.process_data2 import ProcessData, Resampler
import pandas as pd, numpy as np
from redis import StrictRedis
from datetime import datetime as dt

Globs.load_vn30_weight()
DIC_WEIGHTS = {k: v for k, v in zip(Globs.VN30_WEIGHT['stock'],Globs.VN30_WEIGHT['weights'])}
DIC_WEIGHTS_VOLUME = {k: v for k, v in zip(Globs.VN30_WEIGHT['stock'],Globs.VN30_WEIGHT['volume'])}
THRESHOLD = 2
MIN_LOT = 0.1
MAX_LOT = 50
INITIATED = True
VN30_LS = DIC_WEIGHTS.keys()

# df = Adapters.load_data_and_preprocess('2025_02_21')
# df.to_pickle('/data/dang/hose500_2025_02_21.pkl')

r = StrictRedis(decode_responses=True)

def timestamp_to_index(timestamp: pd.Series, unit='s', tz='VN'):

    if unit == 's':
        unit_in_seconds = 1
    elif unit == 'ms':
        unit_in_seconds = 1e-3
    elif unit == 'us':
        unit_in_seconds = 1e-6
    elif unit == 'ns':
        unit_in_seconds = 1e-9
    else:
        raise ValueError('unit must be one of s, ms, us, ns')
    
    if tz == 'VN':
        timestamp = timestamp + 7*3600

    index = (timestamp // unit_in_seconds) % 86400
    return index


def data_adapter():
    day = '2025_02_24'
    df = Adapters.RedisAdapters.load_realtime_stock_data_from_redis(day=day, start=0, end=300000)

    df = df[df['code'].isin(VN30_LS) & (df['lastPrice'] != 0)].copy()

    df['timestamp'] = df['timestamp'] // 1000 + 7*60*60
    df['time'] = df['timestamp'] % 86400

    df = df.rename(columns=ProcessData.REALTIME_HOSE500_COLUMNS_MAPPING)
    df = ProcessData.ensure_all_stocks_present(df, VN30_LS)
    df = df.sort_values(['timestamp', 'totalMatchVolume'])

    df['price_diff'] = df.groupby('stock')['close'].diff()
    df = df.dropna(subset=['price_diff'])
    bu_cond = (df['price_diff'] > 0)
    sd_cond = (df['price_diff'] < 0)
    df['matchedBy2'] = np.where(bu_cond, 1, np.where(sd_cond, -1, np.NaN))
    df['matchedBy2'] = df.groupby('stock')['matchedBy2'].ffill()
    df['matchedBy2'] = df['matchedBy2'].fillna(0)

    df['matchedBy'] = df['matchedBy2'].copy()

    df = Resampler.cleanup_timestamp(day, df)

    df['x'] = df['time']
    df['code'] = df['stock']

    return df

def compute_df_cut(
    df: pd.DataFrame = None,
    upper_bound=12.0,
    lower_bound=8.0,
    bid_or_offer='bid',
    # do_split_order = False
):

    bid_or_offer = bid_or_offer.lower().capitalize()
    lookup = {'Bid': 1, 'Offer': -1}

    # ############## df_matched ##############
    # def split_matched_orders_by_side():
    #     df_matched = df.groupby(['matchedBy', 'code', 'time'])\
    #         .agg({'matchingVolume': sum})\
    #         .loc[lookup[bid_or_offer]]
        
    #     df_matched['matchedVolume'] = df_matched\
    #         .groupby(level='code')\
    #         .agg({'matchingVolume': 'cumsum'})
        
    #     df_matched_lookup: pd.DataFrame = df_matched\
    #         .reset_index()\
    #         .pivot(index='time', columns='code', values='matchedVolume')\
    #         .fillna(method='ffill').fillna(0)
        
    #     df_matched_lookup = df_matched_lookup\
    #         .unstack()\
    #         .to_frame()\
    #         .rename(columns={0: 'accMatchingVolume'})
        
    #     return df_matched_lookup

    df_cut: pd.DataFrame = df.copy()    
    df_cut = df_cut.sort_values(['code','totalMatchVolume'])
    df_cut['real_MatchingVolume'] = df_cut['totalMatchVolume'].diff()
    df_cut['real_MatchingVolume'] = np.where(df_cut['real_MatchingVolume']<0,0,df_cut['real_MatchingVolume'])
    df_cut['real_MatchingVolume'].iloc[0] = 0

    if bid_or_offer != 'Bid':
        df_cut[['bestBid1', 'bestBid1Volume']] = df_cut[['bestOffer1', 'bestOffer1Volume']].copy()
    df_cut['bestBid1'] = df_cut['refPrice']
    df_cut = df_cut.groupby(['code', 'time']).agg({'bestBid1Volume': 'last', 'x': 'last', 'code': 'last', 'time': 'last', 'bestBid1': 'last', 'totalMatchVolume': 'last', 'real_MatchingVolume': 'sum'})
    # if do_split_order:
    #     df_cut = pd.merge(left=df_cut, right=split_matched_orders_by_side(),
    #                       how='left',
    #                       left_index=True, right_index=True)
    #     df_cut['bestBid1Volume'] += df_cut['accMatchingVolume']
    # else:
    df_cut['bestBid1Volume'] += df_cut['totalMatchVolume']
    df = None

    ################ df_pred ###############
    df_cut['weight'] = df_cut['code'].map(lambda x: DIC_WEIGHTS[x]/100)
    df_cut['fakeVolume'] = df_cut['bestBid1Volume'].groupby(level='code').diff().fillna(0)
    df_cut['value'] = df_cut['fakeVolume'] * df_cut['bestBid1']
    df_cut['pred'] = (df_cut['value'] / df_cut['weight']).map(lambda x: round(x / 1_000_000_000, 2))

    ############ filter by size ############
    df_cut.loc[df_cut['pred'] > upper_bound, 'pred'] = 0
    df_cut.loc[df_cut['pred'] < lower_bound, 'pred'] = 0
    df_cut = df_cut[df_cut['fakeVolume'] > 0]
    # print(df_cut.columns)
    df_cut = df_cut[df_cut['pred'] > 0].copy()
    # print(df_cut.columns)
    df_cut['real_MatchingVolume'] = df_cut[['real_MatchingVolume','fakeVolume']].values.min(1)

    return df_cut

def load_arbit_unwind_df(df: pd.DataFrame, atype='arbit', min_lot=0.1, max_lot=200, threshold=None):

    def test():
        df = data_adapter()
        min_lot=0.1
        max_lot=200

    if atype == 'arbit':
        df_cut = compute_df_cut(df=df,
                                lower_bound=min_lot, 
                                upper_bound=max_lot,
                                bid_or_offer='bid')
        df_cut['chart'] = 'Bid'
    
    if atype == 'unwind':
        df_cut = compute_df_cut(df=df,
                                lower_bound=min_lot,
                                upper_bound=max_lot,
                                bid_or_offer='offer')
        df_cut['chart'] = 'Offer'

    return df_cut
    
def find_arbit_unwind(df: pd.DataFrame, pattern_type = 'arbit'):
    def test():
        df = data_adapter()
        pattern_type = 'arbit'

    """
    Phân tích dữ liệu để tìm mẫu arbitrage hoặc unwind
    Args:
        df: DataFrame chứa dữ liệu giao dịch đã được xử lý
        pattern_type: 'arbit' hoặc 'unwind'
    Returns:
        list: Danh sách các nhóm giao dịch thỏa mãn điều kiện
    """
    # 1. Chuẩn bị dữ liệu
    df_analysis:pd.DataFrame = load_arbit_unwind_df(df, pattern_type)
    df_analysis.reset_index(drop=True, inplace=True)
    df_analysis.sort_values('x', inplace=True)
    df_analysis.reset_index(drop=True, inplace=True)

    # Các tham số cấu hình
    TIME_WINDOW = 25        # Cửa sổ thời gian phân tích (giây)
    MIN_STOCKS = 10         # Số lượng mã tối thiểu trong một nhóm
    TIME_EXTEND = 10        # Thời gian tối đa để mở rộng nhóm
    MAX_LOT_DIFF = 0.35    # Chênh lệch số lot tối đa cho phép
    MAX_NORM_LOSS = 6      # Ngưỡng độ lệch chuẩn hóa tối đa

    # Tạo khoảng chia bin cho số lot (tăng theo cấp số nhân)
    LOT_BINS = [0.025 * 1.25 ** x for x in range(1, 50)]

    # Kiểm tra dữ liệu đầu vào
    if len(df_analysis.index) == 0:
        return []

    # 2. Tạo điểm quét thời gian (mỗi 5 giây)
    time_points = list(range(
        df_analysis['x'].min() + TIME_WINDOW, 
        df_analysis['x'].max(), 
        5
    ))

    # 3. Khởi tạo danh sách kết quả
    detected_groups = []

    # 4. Quét qua từng điểm thời gian
    for current_time in time_points:
        # Lấy dữ liệu trong cửa sổ thời gian
        window_data: pd.DataFrame = df_analysis[
            (df_analysis['x'] > current_time - TIME_WINDOW) & 
            (df_analysis['x'] <= current_time)
        ].copy()

        if len(window_data.index) == 0:
            continue

        # Tạo mã định danh cho từng giao dịch
        window_data['timecode'] = window_data['time'].astype(str) + window_data['code']
        window_data['volume_code'] = window_data['fakeVolume'].astype(int).astype(str) + window_data['code']
        window_data['real_volume_code'] = window_data['real_MatchingVolume'].astype(int).astype(str) + window_data['code']

        # Loại bỏ các mã đã được phát hiện trước đó
        used_timecodes = [code for group in detected_groups for code in group['timecodes']]
        window_data = window_data[~window_data['timecode'].isin(used_timecodes)]

        # 5. Mở rộng nhóm gần nhất nếu có thể
        if detected_groups:
            last_group = detected_groups[-1]
            potential_additions = window_data[
                ~window_data['code'].isin(last_group['codes']) &
                (window_data['x'] - last_group['x'] < TIME_EXTEND)
            ].copy()

            if len(potential_additions) > 0:
                # Tính toán độ chênh lệch số lot
                potential_additions['lot_diff'] = abs(
                    potential_additions['pred'] - last_group['num_lot']
                ) / last_group['num_lot']

                # Lọc và thêm vào nhóm gần nhất
                valid_additions = potential_additions[
                    potential_additions['lot_diff'] < MAX_LOT_DIFF
                ].sort_values('lot_diff').drop_duplicates('code')

                if len(valid_additions) > 0:
                    last_group['codes'].extend(valid_additions['code'].tolist())
                    last_group['timecodes'].extend(valid_additions['timecode'].tolist())
                    last_group['volume_codes'].extend(valid_additions['volume_code'].tolist())
                    last_group['real_volume_codes'].extend(valid_additions['real_volume_code'].tolist())
                    detected_groups[-1] = last_group

                    # Cập nhật lại dữ liệu window loại bỏ các mã vừa thêm
                    window_data = window_data[
                        ~window_data['timecode'].isin(valid_additions['timecode'])
                    ]

        # 6. Phân tích nhóm mới
        # Phân nhóm theo số lot
        window_data['lot_group'] = pd.cut(window_data['pred'], bins=LOT_BINS)
        window_data['lot_group'] = window_data['lot_group'].cat.codes
        group_counts = window_data['lot_group'].value_counts()

        # Kiểm tra số lượng mã tối thiểu
        try:
            max_count = group_counts.values[0]
            if max_count < MIN_STOCKS:
                if len(group_counts) < 2:
                    continue
                if group_counts.values[0] + group_counts.values[1] < MIN_STOCKS:
                    continue
        except:
            continue

        # Lọc các mã trong khoảng lot phù hợp
        main_group = group_counts.index[0]
        window_data = window_data[
            (window_data['lot_group'] >= main_group - 1) & 
            (window_data['lot_group'] <= main_group + 1)
        ]

        # 7. Tính toán độ lệch chuẩn
        median_lot = window_data['pred'].median()
        median_time = window_data['x'].median()

        # Tính loss cho từng mã
        window_data['lot_loss'] = (abs((window_data['pred'] - median_lot) / median_lot) + 1) ** 5
        window_data['time_loss'] = (abs(window_data['x'] - median_time)) ** 2 / 100
        window_data['total_loss'] = window_data['time_loss'] + window_data['lot_loss']

        # Chuẩn hóa loss
        n_samples = len(window_data)
        normal_samples = window_data['total_loss'].values[:int(n_samples * 0.8)]
        mu = np.mean(normal_samples)
        std = np.std(normal_samples)
        window_data['norm_loss'] = (window_data['total_loss'] - mu).abs() / std

        # Lọc theo độ lệch chuẩn và loại bỏ trùng lặp
        window_data = window_data[window_data['norm_loss'] < MAX_NORM_LOSS]
        window_data = window_data.drop_duplicates('code', keep='first')

        # 8. Kiểm tra tính nhất quán
        window_data = window_data.sort_values('x')
        time_consistency = (
            window_data['x'].rolling(5).mean() - window_data['x']
        ).abs().min()

        window_data = window_data.sort_values('pred')
        lot_consistency = (
            (window_data['pred'].rolling(5).mean() - window_data['pred']) / median_lot
        ).abs().min()

        if time_consistency / median_lot > 1 or lot_consistency > 1:
            continue

        # 9. Lưu kết quả nếu đủ điều kiện
        if len(window_data.index) >= MIN_STOCKS + 3:
            new_group = {
                'time': window_data['time'].values[0],
                'type': pattern_type,
                'num_lot': median_lot,
                'x': int(window_data['x'].mean()),
                'codes': window_data['code'].tolist(),
                'timecodes': window_data['timecode'].tolist(),
                'volume_codes': window_data['volume_code'].tolist(),
                'real_volume_codes': window_data['real_volume_code'].tolist()
            }
            detected_groups.append(new_group)

    result = []
    for group in detected_groups:
        if (group['num_lot'] >= 8 and len(group['codes']) >= 16) or len(group['codes']) >= 16:
            result.append(group)

    return result

def total_arbit_unwind(df: pd.DataFrame=None, day=None):
    if day is None:
        day = dt.now().strftime('%Y_%m_%d')
    
    if df is None:
        df = data_adapter()

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

def process_arbit_unwind_dict(dict: dict):
    """
    Process arbitrage dictionary into DataFrame format
    
    Args:
        dic (list): List of arbitrage opportunities
        
    Returns:
        pd.DataFrame: Processed data
    """



    




arbit_ls = find_arbit_unwind(
    df=data_adapter(),
    pattern_type='arbit'
)

df_arbit = pd.DataFrame(arbit_ls).sort_values(by='time').reset_index(drop=True)