"""import library"""

from celery import Celery
from redis import Redis
from danglib.pylabview2.funcs import Vectorized, Conds
import logging
import numpy as np
import pandas as pd
from danglib.lazy_core import gen_plasma_functions
import threading
import pickle

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
    COMPUTE_MULTI_STRATEGIES_URSI = 'compute_multi_strategies_ursi'
    COMPUTE_SIGNAL = 'compute_signal'

@app.task(name=TaskName.COMPUTE_SIGNAL)
def compute_signal(idx, params: dict):

    _, disconnect, psave, pload = gen_plasma_functions(db=5)

    df: pd.DataFrame = pload("df_stocks")
    print(df.shape)

    try:
        for k, v in params.items():
            for patt in ['len', 'bar', 'lkbk', 'mult', 'period', 'ranking']:
                k: str
                if patt in k:
                    params[k] = int(v)
        params['use_flag'] = True

        signals: pd.DataFrame = Conds.compute_any_conds(df, **params)
        signals = signals[signals.index >= '2018_01_01']
        signals.iloc[-16:] = False
    except Exception as e:
        print(f"{idx} error: {e}")
        signals = None
    
    disconnect()
    return signals
    

@app.task(name=TaskName.COMPUTE_STOCK)
def scan_one_stock(conds1, conds2):
    with plasma_lock:
        _, _, psave, pload, disconect = gen_plasma_functions(db=5)

        re_2d = pload("return_array")
        disconect()

    return Vectorized.compute_one_strategy(conds1=conds1, conds2=conds2, re_2d=re_2d)

@app.task(name=TaskName.COMPUTE_MULTI_STRATEGIES)
def compute_multi_strategies(i):

    _, disconnect, psave, pload = gen_plasma_functions(db=5)

    array_3d = pload("sig_3d_array")
    re_2d = pload("return_array")
    

    df1 = array_3d[i]

    nt_ls=[]
    re_ls=[]
    wt_ls=[]
    for j in range(i+1, len(array_3d)):
        df2 = array_3d[j]

        if len(df2) == 0:  # Break the loop if there are no more pairs to process
            break

        cond = df1 * df2
        num_trade = np.sum(cond, axis=0)
        re = np.nan_to_num(re_2d * cond, 0.0)
        total_re = np.sum(re, axis=0)
        num_win = np.sum(re > 0, axis=0)

        nt_ls.append(num_trade)
        re_ls.append(total_re)
        wt_ls.append(num_win)

    disconnect()

    nt_arr = np.vstack(nt_ls)
    re_arr = np.vstack(re_ls)
    wt_arr = np.vstack(wt_ls)


    with open(f"/data/dang/tmp/combo_{i}.pkl", 'wb') as f:
        pickle.dump((nt_arr, re_arr, wt_arr), f)
    # return nt_ls, re_ls, wt_ls


@app.task(name=TaskName.COMPUTE_MULTI_STRATEGIES_URSI)
def compute_multi_strategies_ursi(i):

    _, disconnect, psave, pload = gen_plasma_functions(db=5)

    array_3d = pload("sig_3d_array")
    ursi_2d = pload("ursi_vectorized")
    

    df1 = array_3d[i]

    stock_count_ls=[]
    ursi_res = []

    for j in range(i+1, len(array_3d)):
        df2 = array_3d[j]

        if len(df2) == 0:  # Break the loop if there are no more pairs to process
            break

        cond = df1 * df2
        stocks_count = np.sum(cond, axis=1)
        stock_count_ls.append(stocks_count)

        s_ursi = np.sum(np.nan_to_num(ursi_2d * cond, 0))
        s2_ursi = np.sum(np.nan_to_num((ursi_2d ** 2) * cond, 0))

        ursi_res.append({'j': j, 'sum': s_ursi, 'sumSquare': s2_ursi})



    disconnect()

    stock_count_arr = np.vstack(stock_count_ls)

    with open(f"/data/dang/tmp2/stocks_count_day/combo_{i}.pkl", 'wb') as f:
        pickle.dump(stock_count_arr, f)

    with open(f"/data/dang/tmp2/ursi_sum/combo_{i}.pkl", 'wb') as f:
        pickle.dump(pd.DataFrame(ursi_res), f)


def test():
    
    _, disconnect, psave, pload = gen_plasma_functions(db=5)

    array_3d = pload("sig_3d_array")
    ursi_2d = pload("ursi_vectorized")

    i =283

    res = []
    for j in range(i+1, len(array_3d)):
        df2 = array_3d[j]

        if len(df2) == 0:  # Break the loop if there are no more pairs to process
            break

        cond = df1 * df2
        s_ursi = np.sum(np.nan_to_num(ursi_2d * cond, 0))
        s2_ursi = np.sum(np.nan_to_num((ursi_2d ** 2) * cond, 0))

        res.append({'j': j, 'sum': s_ursi, 'sumSquare': s2_ursi})

    disconnect()

    res = pd.DataFrame(res)
    with open(f"/data/dang/tmp2/ursi_sum/combo_{i}.pkl", 'wb') as f:
        pickle.dump(res, f)





# celery -A celery_worker worker --concurrency=10 --loglevel=INFO -n celery_worker@pylabview