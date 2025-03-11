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



def group_conditions(conditions_params: dict):
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
                'function': 'two_line_pos',
                'inputs': {
                    'src1': 'bu',
                    'src2': 'sd',
                    'stocks': Globs.SECTOR_DIC['VN30'],
                },
                'params': {
                    'direction': 'crossover'
                }
            },
        ]

    
    if not conditions_params:
        return None
        
    # Load stock data
    required_data, updated_params = CombiConds.load_and_process_group_data2(conditions_params, realtime=True)
    
    # Generate signals
    signals = CombiConds.combine_conditions(required_data, updated_params)

    return signals

def other_conditions(conditions_params: dict):
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
    required_data, updated_params = CombiConds.load_and_process_one_series_data(conditions_params, use_sample_data=False, realtime=True)
    
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

group_conds = ['BidAskCS', 'BUSD', 'FBuySell', ]
other_conds = ['F1', 'VN30', 'VNINDEX', 'BidAskF1', 'ArbitUnwind', 'PremiumDiscount']

import json
@app.task(name=TaskName.COMPUTE_SIGNALS)
def compute_signals(
    strategy: Dict[str, Any],
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
            signals = group_conditions(strategy['conditions'])
        else:
            signals = other_conditions(strategy['conditions'])
        
        signals.name = 'signals'
        df = signals.to_frame().reset_index()
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

        return df
    except Exception as e:
        logging.error(f"Error in compute_signals: {e}")
        logging.error(traceback.format_exc())
        return {}
    
# celery -A pslab_worker worker --concurrency=20 --loglevel=INFO -n celery_worker@pslab