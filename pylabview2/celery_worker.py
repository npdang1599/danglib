"""import library"""

from celery import Celery
from redis import Redis
from danglib.pylabview2.funcs import Vectorized, Conds
import logging
import numpy as np
import pandas as pd
from danglib.lazy_core import gen_plasma_functions, maybe_create_dir
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
    COMPUTE_MULTI_STRATEGIES_2 = 'compute_multi_strategies_2'
    COMPUTE_MULTI_STRATEGIES_URSI = 'compute_multi_strategies_ursi'
    COMPUTE_MULTI_STRATEGIES_RUDD = 'compute_multi_strategies_ru_dd'
    COMPUTE_MULTI_STRATEGIES_SHARPE = 'compute_multi_strategies_sharpe'
    COMPUTE_MULTI_STRATEGIES_UPDOWN_TREND = 'compute_multi_strategies_utdt'
    COMPUTE_SIGNAL = 'compute_signal'
    COMPUTE_STATS_YEARLY = 'compute_stats_yearly'

@app.task(name=TaskName.COMPUTE_SIGNAL)
def compute_signal(idx, params: dict):

    _, disconnect, psave, pload = gen_plasma_functions(db=5)

    df: pd.DataFrame = pload("df_stocks")
    print(df.shape)

    try:
        for k, v in params.items():
            for patt in ['len', 'bar', 'lkbk', 'period', 'ranking']:
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
    

@app.task(name=TaskName.COMPUTE_MULTI_STRATEGIES)
def compute_multi_strategies(i, folder):

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

    with open(f"{folder}/combo_{i}.pkl", 'wb') as f:
        pickle.dump((nt_arr, re_arr, wt_arr), f)

@app.task(name=TaskName.COMPUTE_MULTI_STRATEGIES_UPDOWN_TREND)
def compute_multi_strategies_utdt(i, folder):

    _, disconnect, psave, pload = gen_plasma_functions(db=5)

    array_3d = pload("sig_3d_array")



    ut_array = pload("ut_array")
    ld_array = pload("ld_array")
    pb_array = pload("pb_array")
    rc_array = pload("rc_array")
    ed_array = pload("ed_array")

    bt_array = pload("before_trough_arr")
    at_array = pload("after_trough_arr")
    bp_array = pload("before_peak_arr")
    ap_array = pload("after_peak_arr")

         


    df1 = array_3d[i]

    ut_ls=[]
    ld_ls=[]
    pb_ls=[]
    rc_ls=[]
    ed_ls=[]

    bt_ls=[]
    at_ls=[]
    bp_ls=[]
    ap_ls=[]

    for j in range(i+1, len(array_3d)):
        df2 = array_3d[j]

        if len(df2) == 0:  # Break the loop if there are no more pairs to process
            break

        cond = df1 * df2
        ut_count = np.sum(cond.T * ut_array, axis=1)
        ld_count = np.sum(cond.T * ld_array, axis=1)
        pb_count = np.sum(cond.T * pb_array, axis=1)
        rc_count = np.sum(cond.T * rc_array, axis=1)
        ed_count = np.sum(cond.T * ed_array, axis=1)

        ut_ls.append(ut_count)
        ld_ls.append(ld_count)
        pb_ls.append(pb_count)
        rc_ls.append(rc_count)
        ed_ls.append(ed_count)

        bt_count = np.sum(cond.T * bt_array, axis=1)
        at_count = np.sum(cond.T * at_array, axis=1)
        bp_count = np.sum(cond.T * bp_array, axis=1)
        ap_count = np.sum(cond.T * ap_array, axis=1)

        bt_ls.append(bt_count)
        at_ls.append(at_count)
        bp_ls.append(bp_count)
        ap_ls.append(ap_count)


    disconnect()

    ut_arr = np.vstack(ut_ls)
    ld_arr = np.vstack(ld_ls)
    pb_arr = np.vstack(pb_ls)
    rc_arr = np.vstack(rc_ls)
    ed_arr = np.vstack(ed_ls)

    bt_arr = np.vstack(bt_ls)
    at_arr = np.vstack(at_ls)
    bp_arr = np.vstack(bp_ls)
    ap_arr = np.vstack(ap_ls)



    with open(f"{folder}/combo_{i}.pkl", 'wb') as f:
        pickle.dump((ut_arr, ld_arr, pb_arr, rc_arr, ed_arr, bt_arr, at_arr, bp_arr, ap_arr), f)


@app.task(name=TaskName.COMPUTE_MULTI_STRATEGIES_2)
def compute_multi_strategies_2(i, start_idx=0, end_idx=-1, folder ='/data/dang/tmp2/nt_wr_re_yearly'):

    _, disconnect, psave, pload = gen_plasma_functions(db=5)

    array_3d = pload("sig_3d_array")
    re_2d = pload("return_array")[start_idx:end_idx]

    df1 = array_3d[i][start_idx:end_idx]

    nt_ls=[]
    re_ls=[]
    wt_ls=[]
    for j in range(i+1, len(array_3d)):
        df2 = array_3d[j][start_idx:end_idx]

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

    f_name = f"{folder}/{start_idx}_{end_idx}"
    maybe_create_dir(f_name)
    with open(f"{f_name}/combo_{i}.pkl", 'wb') as f:
        pickle.dump((nt_arr, re_arr, wt_arr), f)


@app.task(name=TaskName.COMPUTE_MULTI_STRATEGIES_RUDD)
def compute_multi_strategies_ru_dd(i, start_idx=0, end_idx=-1, folder ='/data/dang/tmp2/ru_dd'):

    def test():
        i = 15
        j = 22
        start_idx = 0 
        end_idx = -1
        
    _, disconnect, psave, pload = gen_plasma_functions(db=5)

    array_3d = pload("sig_3d_array")
    ru_2d = pload("runup_vectorized")[start_idx:end_idx]
    dd_2d = pload("drawdown_vectorized")[start_idx:end_idx]

    df1 = array_3d[i][start_idx:end_idx]

    ru_ls=[]
    dd_ls=[]
    for j in range(i+1, len(array_3d)):
        df2 = array_3d[j][start_idx:end_idx]

        if len(df2) == 0:  # Break the loop if there are no more pairs to process
            break

        cond = df1 * df2
        avg_ru = np.sum(np.nan_to_num(ru_2d * cond, 0.0))
        avg_dd = np.sum(np.nan_to_num(dd_2d * cond, 0.0))

        ru_ls.append(avg_ru)
        dd_ls.append(avg_dd)

    disconnect()

    ru_arr = np.vstack(ru_ls)
    dd_arr = np.vstack(dd_ls)


    f_name = f"{folder}/{start_idx}_{end_idx}"
    maybe_create_dir(f_name)
    with open(f"{f_name}/combo_{i}.pkl", 'wb') as f:
        pickle.dump((ru_arr, dd_arr), f)


@app.task(name=TaskName.COMPUTE_MULTI_STRATEGIES_SHARPE)
def compute_multi_strategies_sharpe(i, start_idx=0, end_idx=-1, folder ='/data/dang/tmp2/sharpe'):

    def test():
        i = 15
        j = 22
        start_idx = 0 
        end_idx = -1
        
    _, disconnect, psave, pload = gen_plasma_functions(db=5)

    array_3d = pload("sig_3d_array")
    re_2d  = pload("return_array")[start_idx:end_idx]
    sharpe_2d = re_2d ** 2

    df1 = array_3d[i][start_idx:end_idx]

    sharpe_ls=[]

    for j in range(i+1, len(array_3d)):
        df2 = array_3d[j][start_idx:end_idx]

        if len(df2) == 0:  # Break the loop if there are no more pairs to process
            break

        cond = df1 * df2
        avg_sharpe = np.sum(np.nan_to_num(sharpe_2d * cond, 0.0), axis=0)
        sharpe_ls.append(avg_sharpe)

    disconnect()

    sharpe_arr = np.vstack(sharpe_ls)

    suffix = ''
    if end_idx != -1:
        suffix = f"_{start_idx}_{end_idx}"
    else:
        suffix = f"_{start_idx}_end"

    f_name = f"{folder}/{suffix}"
    maybe_create_dir(f_name)
    with open(f"{f_name}/combo_{i}.pkl", 'wb') as f:
        pickle.dump(sharpe_arr, f)


@app.task(name=TaskName.COMPUTE_MULTI_STRATEGIES_URSI)
def compute_multi_strategies_ursi(i, folder="/data/dang/tmp2"):

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

    with open(f"{folder}/stocks_count_day/combo_{i}.pkl", 'wb') as f:
        pickle.dump(stock_count_arr, f)

    with open(f"{folder}/ursi_sum/combo_{i}.pkl", 'wb') as f:
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