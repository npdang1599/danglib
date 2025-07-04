"""Import libraries"""
import traceback
from celery import Celery
from redis import Redis
from danglib.pslab.funcs import Globs, CombiConds, ReturnStatsConfig, ReturnStats, Adapters, Resampler
from danglib.pslab.utils import Utils


from typing import List, Dict, Any
import pandas as pd
import logging

true = True
false = False

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

@app.task(name=TaskName.COMPUTE_SIGNALS)
def compute_signals(
    strategy: dict,
    realtime: bool = False
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
        conditions = strategy['conditions']
        signals: pd.Series = CombiConds.compute_all_conditions(
            **conditions, realtime=realtime,
        )
        
        signals.name = 'signals'
        df = signals.to_frame().reset_index()
        df['candleTime'] = df['candleTime'] // 1e9
        df['exit_stamp'] = df['candleTime'].shift(-conditions['lookback_periods']-1)
        df = df.rename(columns={'candleTime': 'entry_stamp'})
        df['entry_stamp'] = df['entry_stamp'].shift(-1)
        df = df[df['signals']].copy()
        df = df.drop(columns=['signals'])

        df['id'] = strategy['id']
        df['name'] = strategy['name']
        df['group'] = ",".join(strategy['group'])
        df['type'] = strategy['type']
        df['Winrate'] = strategy['Win Rate']
        df['num_trades'] = strategy['Number of Trades']
        df['num_entry_days'] = strategy['Number Entry Days']
        df['avg_return'] = strategy['Average Return']
        df['holding_periods'] = strategy['holding_periods']
        df = df.dropna(subset=['entry_stamp'])

        return df
    except Exception as e:
        logging.error(f"Error in compute_signals: {e} {strategy}")
        logging.error(traceback.format_exc())
        return {}