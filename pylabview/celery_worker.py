"""import library"""

from celery import Celery
from redis import Redis
from danglib.pylabview.funcs import Simulator, pd
import logging

class CELERY_RESOURCES:
    HOST = 'localhost'
    CELERY_INPUT_REDIS = 8

def clean_redis():
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
    SCAN_STOCK = 'scan_one_stock'
    
@app.task(name=TaskName.SCAN_STOCK)
def scan_one_stock(df: pd.DataFrame, func, params, name="", trade_direction='Long', holding_periods=15):
    bt = Simulator(
        func,
        df_ohlcv=df,
        params=params,
        name=name,
    )
    try:
        bt.run(trade_direction=trade_direction, holding_periods=holding_periods)
    except Exception as e:
        print(f"scan error: {e}")

    return bt
    
    
    
# celery -A celery_worker worker --concurrency=10 --loglevel=INFO -n celery_worker@pylabview