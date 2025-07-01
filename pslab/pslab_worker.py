"""Import libraries"""
import traceback
from celery import Celery
from redis import Redis
from danglib.pslab.funcs import Globs, CombiConds, ReturnStatsConfig, ReturnStats, Adapters, Resampler
from danglib.pslab.utils import Utils


from typing import List, Dict, Any
import pandas as pd
import logging

class CELERY_RESOURCES:
    HOST = 'localhost'
    CELERY_INPUT_REDIS = 1

def clean_redis():
    """Xóa tất cả dữ liệu trong Redis để chuẩn bị tài nguyên cho các tác vụ Celery mới.

    Hàm này kết nối đến Redis tại các cơ sở dữ liệu `CELERY_INPUT_REDIS` và `CELERY_INPUT_REDIS + 1`,
    sau đó xóa tất cả các khóa (keys) hiện có trong cả hai cơ sở dữ liệu để đảm bảo rằng không còn dữ liệu cũ nào còn lại.

    Returns:
        int: Số lượng khóa đã xóa từ Redis.
    """
    r_input = Redis(
        CELERY_RESOURCES.HOST,
        db=CELERY_RESOURCES.CELERY_INPUT_REDIS,
        decode_responses=True)
    r_output = Redis(
        CELERY_RESOURCES.HOST,
        db=CELERY_RESOURCES.CELERY_INPUT_REDIS + 1,
        decode_responses=True)
    count = 0   
    for key in r_output.keys():
        count += r_output.delete(key)
    for key in r_input.keys():
        count += r_input.delete(key)
    return count

def app_factory(host):
    broker_url = f'redis://{host}:6379/' \
                    f'{CELERY_RESOURCES.CELERY_INPUT_REDIS}'
    backend_url = f'redis://{host}:6379/' \
                    f'{CELERY_RESOURCES.CELERY_INPUT_REDIS + 1}'
    remote_app = Celery(
        'dc_slavemaster',
        broker=broker_url, 
        backend=backend_url)
    remote_app.conf.task_serializer = 'pickle'
    remote_app.conf.result_serializer = 'pickle'
    remote_app.conf.accept_content = [
        'application/json',
        'application/x-python-serialize']
    return remote_app

app = app_factory(CELERY_RESOURCES.HOST)

class TaskName:
    COMPUTE_SIGNALS = 'compute_signals'
    RUN_ANY_FUNCIONS = 'run_any_functions'

def fix_conditions_params(conditions_params):
    map = {
        'F1': 'F1',
        'VN30': 'Vn30',
        'VNINDEX': 'Vnindex'
    }

    for condition in conditions_params:
        for key, value in condition["inputs"].items():
            if isinstance(value, str): 
                for old, new in map.items():
                    if old in value: 
                        condition["inputs"][key] = value.replace(old, new)



def group_conditions(conditions_params: dict, realtime=True, data=None):
    """
        Process strategies that use stock data.
        
        Args:
            conditions_params: List of strategies with stocks
            
        Returns:
            pd.Series of boolean signals
    """
    def test():
        conditions_params = [
            {
                'function': 'absolute_change_in_range',
                'inputs': {'src': 'bid',
                    'timeframe': '30S',
                    'rolling_window': 1,
                    'rolling_method': 'mean',
                    'daily_rolling': False,
                    'stocks': ['All']},
                'params': {'n_bars': 20,
                    'lower_thres': 380.0,
                    'upper_thres': 1000000000000000000,
                    'use_as_lookback_cond': False,
                    'lookback_cond_nbar': 5}
                    }
        ]
        realtime = False
        data = None

    
    if not conditions_params:
        return None
        
    # Load stock data
    required_data, updated_params = CombiConds.load_and_process_group_data(conditions_params, realtime=realtime, data=data)
    
    # Generate signals
    signals = CombiConds.combine_conditions(required_data, updated_params)

    return signals

def other_conditions(conditions_params: dict, realtime=True, data = None):
    """
        Process strategies that use one series data.
        
        Args:
            conditions_params: List of strategies without stocks
            
        Returns:
            pd.Series of boolean signals
    """
    if not conditions_params:
        return None
    
    fix_conditions_params(conditions_params)
        
    # Load one series data
    required_data, updated_params = CombiConds.load_and_process_one_series_data(conditions_params, realtime=realtime, data=data)
    # print('required_data', required_data)
    # a = pd.DataFrame(required_data)
    # a.index = pd.to_datetime(a.index / 1e9 , unit='s') 
    # if realtime:
    #     name = 'realtime'
    # else:
    #     name = 'backtest'
    # filename = f'/home/ubuntu/hip/{name}_{a.columns[0]}.xlsx'
    # a.to_excel(filename, index=True)
    # print('updated_params', updated_params)
    # Generate signals
    signals = CombiConds.combine_conditions(required_data, updated_params)
    
    return signals

def calculate_current_candletime(timestamps: float, timeframe: str, unit='s') -> float:
    """Calculate base candle times"""
    tf_seconds = Globs.TF_TO_MIN.get(timeframe) * 60
    return (timestamps // tf_seconds) * tf_seconds

def calculate_finished_candletime(timestamps:float,  timeframe: str, unit='s'):
    """Calculate finished candle times"""
    return calculate_current_candletime(timestamps, timeframe) - Globs.TF_TO_MIN.get(timeframe) * 60

def calculate_next_candletime(timestamps: float, timeframe: str, unit='s'):
    """Calculate next candle times"""
    return calculate_current_candletime(timestamps, timeframe) + Globs.TF_TO_MIN.get(timeframe) * 60

group_conds = ['BidAskCS', 'BUSD', 'FBuySell']
other_conds = ['F1', 'VN30', 'VNINDEX', 'BidAskF1', 'ArbitUnwind', 'PremiumDiscount', 'PS', 'BuySellImpact']

import json
@app.task(name=TaskName.COMPUTE_SIGNALS)
def compute_signals(
    strategy: Dict[str, Any],
    realtime: bool = True,
    group_data: pd.DataFrame = None,
    market_data: pd.DataFrame = None
) -> Dict[str, Any]:
    """Tính toán tín hiệu giao dịch dựa trên điều kiện và tham số đầu vào.

    Args:
        data (Dict[str, Any]): Dữ liệu đầu vào.
        conditions (List[Dict[str, Any]]): Danh sách các điều kiện giao dịch.
        group (int): Nhóm giao dịch.
        direction (str): Hướng giao dịch
        ftype (str): Loại giao dịch.
        holding_periods (int): Số lượng thanh khoản.

    Returns:
        Dict[str, Any]: Kết quả tính toán.
    """
    try:
        group = strategy['group']
        if group in group_conds:
            signals = group_conditions(strategy['conditions'], realtime, data = group_data)
        else:
            signals = other_conditions(strategy['conditions'], realtime, data = market_data)
        
        signals.name = 'signals'
        df = signals.to_frame().reset_index()
        df['candleTime'] = df['candleTime'] // 1e9
        df['exit_stamp'] = df['candleTime'].shift(-strategy['holding_periods']-1)
        df = df.rename(columns={'candleTime': 'entry_stamp'})
        df['entry_stamp'] = df['entry_stamp'].shift(-1)
        df = df[df['signals']].copy()
        df = df.drop(columns=['signals'])

        df['name'] = strategy['name']
        df['group'] = strategy['group']
        df['type'] = strategy['type']
        df['ftype'] = strategy['ftype']
        df['Winrate'] = strategy['Win Rate']
        df['num_trades'] = strategy['Number of Trades']
        df['num_entry_days'] = strategy['Number Entry Days']
        df['avg_return'] = strategy['Average Return']
        df['holding_periods'] = strategy['holding_periods']
        df['rolling_window'] = strategy['rolling_window']
        df = df.dropna(subset=['entry_stamp'])

        return df
    except Exception as e:
        logging.error(f"Error in compute_signals: {e} {strategy}")
        logging.error(traceback.format_exc())
        return {}
    

def compute_signals_no_catch_error(
    strategy: Dict[str, Any],
    realtime: bool = True,
    group_data: pd.DataFrame = None,
    market_data: pd.DataFrame = None
) -> Dict[str, Any]:
    """Tính toán tín hiệu giao dịch dựa trên điều kiện và tham số đầu vào.

    Args:
        data (Dict[str, Any]): Dữ liệu đầu vào.
        conditions (List[Dict[str, Any]]): Danh sách các điều kiện giao dịch.
        group (int): Nhóm giao dịch.
        direction (str): Hướng giao dịch
        ftype (str): Loại giao dịch.
        holding_periods (int): Số lượng thanh khoản.

    Returns:
        Dict[str, Any]: Kết quả tính toán.
    """

    def test():
        strategy ={'name': 2311521627,
            'conditions': [{'function': 'absolute_change_in_range',
                'inputs': {'src': 'bid',
                'timeframe': '30S',
                'rolling_window': 120,
                'rolling_method': 'mean',
                'daily_rolling': False,
                'stocks': ['All']},
                'params': {'n_bars': 1,
                'lower_thres': -1000000000000000000,
                'upper_thres': -4.0,
                'use_as_lookback_cond': False,
                'lookback_cond_nbar': 5}}],
            'holding_periods': 30,
            'Number of Trades': 355,
            'Number Entry Days': 33,
            'Win Rate': 28.169014084507,
            'Average Return': -2.86169014084507,
            'avghigh': 2.33267605633804,
            'avglow': -6.25718309859154,
            'group': 'BidAskCS',
            'type': 'short',
            'ftype': 'single'}
        group_data: pd.DataFrame = None
        market_data: pd.DataFrame = None

    group = strategy['group']
    if group in group_conds:
        signals = group_conditions(strategy['conditions'], realtime, data = group_data)
    else:
        signals = other_conditions(strategy['conditions'], realtime, data = market_data)
    
    signals.name = 'signals'
    df = signals.to_frame().reset_index()
    df['candleTime'] = df['candleTime'] // 1e9
    df['exit_stamp'] = df['candleTime'].shift(-strategy['holding_periods']-1)
    df = df.rename(columns={'candleTime': 'entry_stamp'})
    df['entry_stamp'] = df['entry_stamp'].shift(-1)
    df = df[df['signals']].copy()
    df = df.drop(columns=['signals'])

    df['name'] = strategy['name']
    df['group'] = strategy['group']
    df['type'] = strategy['type']
    df['ftype'] = strategy['ftype']
    df['Winrate'] = strategy['Win Rate']
    df['num_trades'] = strategy['Number of Trades']
    df['num_entry_days'] = strategy['Number Entry Days']
    df['avg_return'] = strategy['Average Return']
    df['holding_periods'] = strategy['holding_periods']
    df['rolling_window'] = strategy['rolling_window']
    df = df.dropna(subset=['entry_stamp'])

    return df

import importlib

@app.task(name=TaskName.RUN_ANY_FUNCIONS)
def run_dynamic_function(function_path: str, *args, **kwargs):
    """
    Chạy bất kỳ hàm nào dựa trên đường dẫn đến hàm và các tham số
    
    :param function_path: Đường dẫn đến hàm (ví dụ: "module.submodule.function")
    :param args: Các tham số positional
    :param kwargs: Các tham số keyword
    :return: Kết quả từ hàm được gọi
    """
    # Tách module path và tên hàm
    module_path, function_name = function_path.rsplit('.', 1)
    
    # Import module động
    module = importlib.import_module(module_path)
    
    # Lấy hàm từ module
    func = getattr(module, function_name)
    
    # Chạy hàm với các tham số được cung cấp
    return func(*args, **kwargs)

@app.task()
def compute_signals_general(
    strategy: Dict[str, Any],
    realtime: bool = True,
    group_data: pd.DataFrame = None,
    market_data: pd.DataFrame = None
) -> Dict[str, Any]:
    """Hàm mới: xử lý cả chiến thuật 1 điều kiện và 2 điều kiện (combine) cho signals."""
    try:
        # Nếu có 2 điều kiện và có group1, group2 thì xử lý riêng biệt và kết hợp bằng AND
        if 'group1' in strategy and 'group2' in strategy and len(strategy.get('conditions', [])) == 2:
            cond1 = [strategy['conditions'][0]]
            cond2 = [strategy['conditions'][1]]
            group1 = strategy['group1']
            group2 = strategy['group2']
            if group1 in group_conds:
                signals1 = group_conditions(cond1, realtime, data=group_data)
            else:
                signals1 = other_conditions(cond1, realtime, data=market_data)
            if group2 in group_conds:
                signals2 = group_conditions(cond2, realtime, data=group_data)
            else:
                signals2 = other_conditions(cond2, realtime, data=market_data)
            # Kết hợp tín hiệu (AND logic)
            signals = Utils.and_conditions([signals1, signals2])
        else:
            # Trường hợp 1 điều kiện hoặc không có group1/group2
            group = strategy['group']
            if group in group_conds:
                signals = group_conditions(strategy['conditions'], realtime, data=group_data)
            else:
                signals = other_conditions(strategy['conditions'], realtime, data=market_data)
        signals.name = 'signals'
        df = signals.to_frame().reset_index()
        df['candleTime'] = df['candleTime'] // 1e9
        df['exit_stamp'] = df['candleTime'].shift(-strategy['holding_periods']-1)
        df = df.rename(columns={'candleTime': 'entry_stamp'})
        df['entry_stamp'] = df['entry_stamp'].shift(-1)
        df = df[df['signals']].copy()
        df = df.drop(columns=['signals'])

        df['name'] = strategy['name']
        df['group'] = strategy.get('group', None)
        df['group1'] = strategy.get('group1', None)
        df['group2'] = strategy.get('group2', None)
        df['type'] = strategy['type']
        df['ftype'] = strategy['ftype']
        df['Winrate'] = strategy.get('Win Rate', None)
        df['num_trades'] = strategy.get('Number of Trades', None)
        df['num_entry_days'] = strategy.get('Number Entry Days', None)
        df['avg_return'] = strategy.get('Average Return', None)
        df['holding_periods'] = strategy['holding_periods']
        df['rolling_window'] = strategy.get('rolling_window', None)
        df['id'] = strategy.get('id', None)
        df = df.dropna(subset=['entry_stamp'])
        return df
    except Exception as e:
        logging.error(f"Error in compute_signals_general: {e} {strategy}")
        logging.error(traceback.format_exc())
        return {}


def compute_signals_no_catch_error_general(
    strategy: Dict[str, Any],
    realtime: bool = True,
    group_data: pd.DataFrame = None,
    market_data: pd.DataFrame = None
) -> Dict[str, Any]:
    """Hàm mới: xử lý cả chiến thuật 1 điều kiện và 2 điều kiện (combine) cho signals."""

    if 'group1' in strategy and 'group2' in strategy and len(strategy.get('conditions', [])) == 2:
            cond1 = [strategy['conditions'][0]]
            cond2 = [strategy['conditions'][1]]
            group1 = strategy['group1']
            group2 = strategy['group2']
            if group1 in group_conds:
                signals1 = group_conditions(cond1, realtime, data=group_data)
            else:
                signals1 = other_conditions(cond1, realtime, data=market_data)
            if group2 in group_conds:
                signals2 = group_conditions(cond2, realtime, data=group_data)
            else:
                signals2 = other_conditions(cond2, realtime, data=market_data)
            # Kết hợp tín hiệu (AND logic)
            signals = Utils.and_conditions([signals1, signals2])
    else:
        # Trường hợp 1 điều kiện hoặc không có group1/group2
        group = strategy['group']
        if group in group_conds:
            signals = group_conditions(strategy['conditions'], realtime, data=group_data)
        else:
            signals = other_conditions(strategy['conditions'], realtime, data=market_data)

    signals.name = 'signals'
    df = signals.to_frame().reset_index()
    df['candleTime'] = df['candleTime'] // 1e9
    df['exit_stamp'] = df['candleTime'].shift(-strategy['holding_periods']-1)
    df = df.rename(columns={'candleTime': 'entry_stamp'})
    df['entry_stamp'] = df['entry_stamp'].shift(-1)
    df = df[df['signals']].copy()
    df = df.drop(columns=['signals'])

    df['name'] = strategy['name']
    df['group'] = strategy.get('group', None)
    df['group1'] = strategy.get('group1', None)
    df['group2'] = strategy.get('group2', None)
    df['type'] = strategy['type']
    df['ftype'] = strategy['ftype']
    df['Winrate'] = strategy.get('Win Rate', None)
    df['num_trades'] = strategy.get('Number of Trades', None)
    df['num_entry_days'] = strategy.get('Number Entry Days', None)
    df['avg_return'] = strategy.get('Average Return', None)
    df['holding_periods'] = strategy['holding_periods']
    df['rolling_window'] = strategy.get('rolling_window', None)
    df['id'] = strategy.get('id', None)
    df = df.dropna(subset=['entry_stamp'])

    return df



# celery -A pslab_worker worker --concurrency=20 --loglevel=INFO -n celery_worker@pslab