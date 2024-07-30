"""import library"""

from celery import Celery
from redis import Redis
from danglib.pylabview2.funcs import Vectorized
import logging
import numpy as np
from dc_server.lazy_core import gen_plasma_functions
import threading

plasma_lock = threading.Lock()

class CELERY_RESOURCES:
    HOST = 'localhost'
    CELERY_INPUT_REDIS = 9

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
    COMPUTE_STOCK = 'compute_one_stock'
    COMPUTE_MULTI_STRATEGIES = 'compute_multi_strategies'
    SCAN_STOCK_V3 = 'scan_one_stock_v3'
    
@app.task(name=TaskName.COMPUTE_STOCK)
def scan_one_stock(conds1, conds2):
    with plasma_lock:
        _, _, psave, pload, disconect = gen_plasma_functions()

        re_2d = pload("return_array")
        disconect()

    return Vectorized.compute_one_strategy(conds1=conds1, conds2=conds2, re_2d=re_2d)

@app.task(name=TaskName.COMPUTE_MULTI_STRATEGIES)
def compute_multi_strategies(sig_dic_num, i, re_2d):
    n = len(sig_dic_num)
    ls = []
    for j in range(i, n):
        cond = sig_dic_num[i] & sig_dic_num[j]
        re = np.sum(np.nan_to_num(cond * re_2d), axis=0)
        ls.append(re)

    return ls





# celery -A celery_worker worker --concurrency=10 --loglevel=INFO -n celery_worker@pylabview