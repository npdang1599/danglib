from danglib.pslab.resources import Adapters, Globs
from danglib.pslab.process_data3 import ProcessStockData as ProcessData, Resampler
import pandas as pd
import numpy as np
from redis import StrictRedis
from datetime import datetime as dt
from typing import Dict, List, Any, Optional, Tuple
import json

# Khởi tạo các biến toàn cục
Globs.load_vn30_weight()
DIC_WEIGHTS = {k: v for k, v in zip(Globs.VN30_WEIGHT['stock'], Globs.VN30_WEIGHT['weights'])}
DIC_WEIGHTS_VOLUME = {k: v for k, v in zip(Globs.VN30_WEIGHT['stock'], Globs.VN30_WEIGHT['volume'])}
THRESHOLD = 2
MIN_LOT = 0.1
MAX_LOT = 50
VN30_LS = list(DIC_WEIGHTS.keys())

# Khởi tạo Redis client
r = StrictRedis(decode_responses=True)

def timestamp_to_index(timestamp: pd.Series, unit='s', tz='VN') -> pd.Series:
    """
    Chuyển đổi timestamp thành index trong ngày (số giây từ 00:00:00)
    
    Args:
        timestamp: Series chứa timestamp
        unit: Đơn vị của timestamp ('s', 'ms', 'us', 'ns')
        tz: Múi giờ ('VN' cho UTC+7)
        
    Returns:
        Series chứa index trong ngày
    """
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


def data_adapter(day: Optional[str] = None) -> pd.DataFrame:
    """
    Đọc và xử lý dữ liệu từ Redis
    
    Args:
        day: Ngày cần lấy dữ liệu (định dạng 'YYYY_MM_DD')
        
    Returns:
        DataFrame đã xử lý
    """
    if day is None:
        day = dt.now().strftime('%Y_%m_%d')
        
    # Lấy dữ liệu từ Redis
    df = Adapters.RedisAdapters.load_realtime_stock_data_from_redis(day=day, start=0, end=-1)

    # Lọc dữ liệu
    df = df[df['code'].isin(VN30_LS) & (df['lastPrice'] != 0)].copy()

    # Xử lý timestamp
    df['timestamp'] = df['timestamp'] // 1000 + 7*60*60
    df['time'] = df['timestamp'] % 86400

    # Đổi tên cột
    df = df.rename(columns=ProcessData.REALTIME_HOSE500_COLUMNS_MAPPING)
    
    # Đảm bảo tất cả mã cổ phiếu đều hiện diện
    df = ProcessData.ensure_all_stocks_present(df, VN30_LS)
    df = df.sort_values(['timestamp', 'totalMatchVolume'])

    # Tính toán chênh lệch giá và xác định matchedBy
    df['price_diff'] = df.groupby('stock')['close'].diff()
    df = df.dropna(subset=['price_diff'])
    bu_cond = (df['price_diff'] > 0)
    sd_cond = (df['price_diff'] < 0)
    df['matchedBy'] = np.where(bu_cond, 1, np.where(sd_cond, -1, np.NaN))
    df['matchedBy'] = df.groupby('stock')['matchedBy'].ffill()
    df['matchedBy'] = df['matchedBy'].fillna(0)

    # Clean up timestamp
    df = Resampler.cleanup_timestamp(day, df)

    # Thêm các cột cần thiết
    df['x'] = df['time']
    df['code'] = df['stock']

    return df


def compute_df_cut(
    df: pd.DataFrame,
    upper_bound: float = 12.0,
    lower_bound: float = 8.0,
    bid_or_offer: str = 'bid'
) -> pd.DataFrame:
    """
    Tính toán và lọc các mã cổ phiếu thỏa mãn điều kiện arbit/unwind
    
    Args:
        df: DataFrame dữ liệu đầu vào
        upper_bound: Giới hạn trên của số lot
        lower_bound: Giới hạn dưới của số lot
        bid_or_offer: 'bid' cho arbit, 'offer' cho unwind
        
    Returns:
        DataFrame đã lọc
    """
    bid_or_offer = bid_or_offer.lower().capitalize()

    # Tạo bản sao dữ liệu
    df_cut: pd.DataFrame = df.copy()    
    df_cut = df_cut.sort_values(['code', 'totalMatchVolume'])
    
    # Tính toán khối lượng giao dịch thực
    df_cut['real_MatchingVolume'] = df_cut['totalMatchVolume'].diff()
    df_cut['real_MatchingVolume'] = np.where(df_cut['real_MatchingVolume'] < 0, 0, df_cut['real_MatchingVolume'])
    df_cut['real_MatchingVolume'].iloc[0] = 0

    # Xử lý theo loại giao dịch (bid/offer)
    if bid_or_offer != 'Bid':
        df_cut[['bestBid1', 'bestBid1Volume']] = df_cut[['bestOffer1', 'bestOffer1Volume']].copy()
    df_cut['bestBid1'] = df_cut['refPrice']
    
    # Nhóm theo mã và thời gian
    df_cut = df_cut.groupby(['code', 'time']).agg({
        'bestBid1Volume': 'last', 
        'x': 'last', 
        'code': 'last', 
        'time': 'last', 
        'bestBid1': 'last', 
        'totalMatchVolume': 'last', 
        'real_MatchingVolume': 'sum'
    })
    
    # Cộng tổng khối lượng
    df_cut['bestBid1Volume'] += df_cut['totalMatchVolume']

    # Tính toán dự đoán
    df_cut['weight'] = df_cut['code'].map(lambda x: DIC_WEIGHTS.get(x, 0)/100)
    df_cut['fakeVolume'] = df_cut['bestBid1Volume'].groupby(level='code').diff().fillna(0)
    df_cut['value'] = df_cut['fakeVolume'] * df_cut['bestBid1']
    df_cut['pred'] = (df_cut['value'] / df_cut['weight']).map(lambda x: round(x / 1_000_000_000, 2))

    # Lọc theo kích thước
    df_cut.loc[df_cut['pred'] > upper_bound, 'pred'] = 0
    df_cut.loc[df_cut['pred'] < lower_bound, 'pred'] = 0
    df_cut = df_cut[df_cut['fakeVolume'] > 0]
    df_cut = df_cut[df_cut['pred'] > 0].copy()
    
    # Tính toán khối lượng thực
    df_cut['real_MatchingVolume'] = df_cut[['real_MatchingVolume', 'fakeVolume']].values.min(1)

    return df_cut


def load_arbit_unwind_df(
    df: pd.DataFrame, 
    atype: str = 'arbit', 
    min_lot: float = 0.1, 
    max_lot: float = 200
) -> pd.DataFrame:
    """
    Tải và xử lý dữ liệu arbit/unwind
    
    Args:
        df: DataFrame dữ liệu đầu vào
        atype: Loại phân tích ('arbit' hoặc 'unwind')
        min_lot: Số lot tối thiểu
        max_lot: Số lot tối đa
        
    Returns:
        DataFrame đã xử lý
    """
    if atype == 'arbit':
        df_cut = compute_df_cut(
            df=df,
            lower_bound=min_lot, 
            upper_bound=max_lot,
            bid_or_offer='bid'
        )
        df_cut['chart'] = 'Bid'
    
    elif atype == 'unwind':
        df_cut = compute_df_cut(
            df=df,
            lower_bound=min_lot,
            upper_bound=max_lot,
            bid_or_offer='offer'
        )
        df_cut['chart'] = 'Offer'

    return df_cut


def find_arbit_unwind(
    df: pd.DataFrame, 
    pattern_type: str = 'arbit'
) -> List[Dict[str, Any]]:
    """
    Phân tích dữ liệu để tìm mẫu arbitrage hoặc unwind
    
    Args:
        df: DataFrame chứa dữ liệu giao dịch đã được xử lý
        pattern_type: 'arbit' hoặc 'unwind'
        
    Returns:
        Danh sách các nhóm giao dịch thỏa mãn điều kiện
    """
    # 1. Chuẩn bị dữ liệu
    df_analysis = load_arbit_unwind_df(df, pattern_type)
    df_analysis.reset_index(drop=True, inplace=True)
    df_analysis.sort_values('x', inplace=True)
    df_analysis.reset_index(drop=True, inplace=True)

    # Các tham số cấu hình
    TIME_WINDOW = 25        # Cửa sổ thời gian phân tích (giây)
    MIN_STOCKS = 10         # Số lượng mã tối thiểu trong một nhóm
    TIME_EXTEND = 10        # Thời gian tối đa để mở rộng nhóm
    MAX_LOT_DIFF = 0.35     # Chênh lệch số lot tối đa cho phép
    MAX_NORM_LOSS = 6       # Ngưỡng độ lệch chuẩn hóa tối đa

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
        window_data = df_analysis[
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
        if n_samples > 1:  # Đảm bảo có ít nhất 2 mẫu để tính std
            normal_samples = window_data['total_loss'].values[:int(max(1, n_samples * 0.8))]
            mu = np.mean(normal_samples)
            std = np.std(normal_samples) if len(normal_samples) > 1 else 1.0
            window_data['norm_loss'] = (window_data['total_loss'] - mu).abs() / (std if std > 0 else 1.0)

            # Lọc theo độ lệch chuẩn và loại bỏ trùng lặp
            window_data = window_data[window_data['norm_loss'] < MAX_NORM_LOSS]
            window_data = window_data.drop_duplicates('code', keep='first')

            # 8. Kiểm tra tính nhất quán
            if len(window_data) >= 5:  # Cần ít nhất 5 mẫu để sử dụng rolling
                window_data = window_data.sort_values('x')
                time_rolling = window_data['x'].rolling(5).mean()
                time_consistency = (time_rolling - window_data['x']).abs().min() if not time_rolling.empty else np.inf

                window_data = window_data.sort_values('pred')
                lot_rolling = window_data['pred'].rolling(5).mean()
                lot_consistency = ((lot_rolling - window_data['pred']) / median_lot).abs().min() if not lot_rolling.empty else np.inf

                if time_consistency / median_lot > 1 or lot_consistency > 1:
                    continue
            else:
                # Bỏ qua kiểm tra tính nhất quán nếu không đủ mẫu
                pass

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

    # Lọc kết quả cuối cùng
    result = []
    for group in detected_groups:
        if (group['num_lot'] >= 8 and len(group['codes']) >= 16) or len(group['codes']) >= 16:
            result.append(group)

    return result


def convert_seconds_to_time(seconds: int) -> str:
    """
    Chuyển đổi số giây trong ngày thành định dạng thời gian HH:MM:SS
    
    Args:
        seconds: Số giây từ 00:00:00
        
    Returns:
        Chuỗi thời gian định dạng HH:MM:SS
    """
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def total_arbit_unwind(
    df: Optional[pd.DataFrame] = None, 
    day: Optional[str] = None
) -> pd.DataFrame:
    """
    Tính toán arbit và unwind theo thời gian
    
    Args:
        df: DataFrame dữ liệu đầu vào (nếu None thì sẽ được tạo từ data_adapter)
        day: Ngày cần tính toán (định dạng 'YYYY_MM_DD')
        
    Returns:
        DataFrame chứa thông tin arbit/unwind theo thời gian với các cột:
        - time: Thời gian định dạng HH:MM:SS
        - timestamp: Unix timestamp (seconds)
        - Arbit: Giá trị누적 arbit tại thời điểm đó
        - Unwind: Giá trị누적 unwind tại thời điểm đó
        - arbit_raw: Giá trị arbit riêng tại thời điểm đó
        - unwind_raw: Giá trị unwind riêng tại thời điểm đó
    """
    if day is None:
        day = dt.now().strftime('%Y_%m_%d')
    
    if df is None:
        df = data_adapter(day)
    
    # Tìm arbit và unwind
    arbit_groups = find_arbit_unwind(df, pattern_type='arbit')
    unwind_groups = find_arbit_unwind(df, pattern_type='unwind')
    
    # Tạo DataFrame từ kết quả arbit
    df_arbit = pd.DataFrame(arbit_groups) if arbit_groups else pd.DataFrame()
    if not df_arbit.empty:
        df_arbit = df_arbit.sort_values(by='time').reset_index(drop=True)
        df_arbit['acc_num_lot'] = df_arbit['num_lot'].cumsum()
        df_arbit = df_arbit[['time', 'num_lot', 'acc_num_lot']].rename(
            columns={'acc_num_lot': 'Arbit', 'num_lot': 'arbit_raw'}
        )
    
    # Tạo DataFrame từ kết quả unwind
    df_unwind = pd.DataFrame(unwind_groups) if unwind_groups else pd.DataFrame()
    if not df_unwind.empty:
        df_unwind = df_unwind.sort_values(by='time').reset_index(drop=True)
        df_unwind['acc_num_lot'] = df_unwind['num_lot'].cumsum()
        df_unwind = df_unwind[['time', 'num_lot', 'acc_num_lot']].rename(
            columns={'acc_num_lot': 'Unwind', 'num_lot': 'unwind_raw'}
        )
    
    # Kết hợp kết quả
    result_df = pd.DataFrame()
    
    if not df_arbit.empty and not df_unwind.empty:
        # Nếu có cả arbit và unwind
        result_df = pd.merge(df_arbit, df_unwind, how='outer', on='time')
    elif not df_arbit.empty:
        # Nếu chỉ có arbit
        result_df = df_arbit
    elif not df_unwind.empty:
        # Nếu chỉ có unwind
        result_df = df_unwind
    else:
        # Nếu không có gì
        return pd.DataFrame(columns=['time', 'time_str', 'timestamp', 'Arbit', 'Unwind', 'arbit_raw', 'unwind_raw'])
    
    # Đảm bảo tất cả các cột đều có
    if 'Arbit' not in result_df.columns:
        result_df['Arbit'] = 0
        result_df['arbit_raw'] = 0
    if 'Unwind' not in result_df.columns:
        result_df['Unwind'] = 0
        result_df['unwind_raw'] = 0
    
    # Lọc và sắp xếp kết quả
    result_df = result_df[result_df['time'] > 0].sort_values('time').reset_index(drop=True)
    
    # Điền vào các giá trị NaN
    result_df['Arbit'] = result_df['Arbit'].fillna(method='ffill').fillna(0)
    result_df['Unwind'] = result_df['Unwind'].fillna(method='ffill').fillna(0)
    result_df['arbit_raw'] = result_df['arbit_raw'].fillna(0)
    result_df['unwind_raw'] = result_df['unwind_raw'].fillna(0)
    
    # Chuyển đổi thời gian
    result_df['time_str'] = result_df['time'].apply(convert_seconds_to_time)
    
    # Tính timestamp (seconds)
    start_timestamp = pd.to_datetime(day, format='%Y_%m_%d').timestamp()
    result_df['timestamp'] = start_timestamp + result_df['time']
    
    # Sắp xếp lại các cột
    result_df = result_df[['time', 'time_str', 'timestamp', 'Arbit', 'Unwind', 'arbit_raw', 'unwind_raw']]
    result_df = result_df.rename(columns={'time_str': 'time'})
    result_df = result_df.drop(columns=['time'])
    
    return result_df



# Demo chạy thử
if __name__ == "__main__":
    # Lấy dữ liệu
    print("Đang tải dữ liệu...")
    day = dt.now().strftime('%Y_%m_%d')
    df = data_adapter(day)
    
    # Tính toán arbit/unwind theo thời gian
    print("Đang tính toán arbit/unwind theo thời gian...")
    result_df = total_arbit_unwind(df, day)
    result_df['dt'] = pd.to_datetime(result_df['timestamp'], unit='s')
    import plotly.graph_objects as go
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=result_df['dt'], y=result_df['Arbit'], name='Arbit'))
    fig.add_trace(go.Scatter(x=result_df['dt'], y=result_df['Unwind'], name='Unwind'))
    fig.update_layout(
        hovermode='x'
    )
    fig.show()