import json, pickle
# noinspection PyUnresolvedReferences
from time import time, sleep
import numpy as np
from datetime import datetime as dt
# noinspection PyUnresolvedReferences
from functools import reduce
# noinspection PyUnresolvedReferences
from typing import Dict, List
# noinspection PyUnresolvedReferences
from copy import copy, deepcopy


def secured_mongo_connector(host='127.0.0.1', port=27017, timeout_ms=2000):
    from pymongo import MongoClient
    # Get credentials from file
    client = MongoClient(
        host=host,
        port=port,
        serverSelectionTimeoutMS=timeout_ms
    )
    return client


def secured_redis_connector(host='127.0.0.1', port=6379, *args, **kwargs):
    from redis import StrictRedis
    # Get credentials from file
    client = StrictRedis(
        host=host,
        port=port,
        *args,
        **kwargs,
    )
    return client


class PALLETE:
    PURPLE = '#856DFC'
    DARK_PURPLE = '#645CDF'
    LIGHT_PURPLE = '#7769F0'
    ORANGE = '#F59200'
    TEAL = '#38D898'
    LIGHT_BLUE = '#2196f3'
    GREEN = '#26A69A'
    RED = '#EF5350'
    NAVY = '#355259'
    DARK_YELLOW = '#B1AF2C'


def request_vps(stock = 'HPG'):
    import requests, pandas as pd
    headers = {
        'Connection': 'keep-alive',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="96", "Google Chrome";v="96"',
        'Accept': 'application/json, text/plain, */*',
        'sec-ch-ua-mobile': '?0',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
        'sec-ch-ua-platform': '"Linux"',
        'Origin': 'https://banggia.vps.com.vn',
        'Sec-Fetch-Site': 'same-site',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        'Referer': 'https://banggia.vps.com.vn/',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    response = requests.get(f'https://bgapidatafeed.vps.com.vn/getliststocktrade/{stock}', headers=headers)
    data = response.json()
    return pd.DataFrame(data)


def make_handle_redis_events():
    class Closure: pass
    closure = Closure()
    closure._count = 0
    def handle_redis_events(msg):
        closure._count += 1
        print(f"\r#{closure._count} {msg}", end="")
    return handle_redis_events


def create_sub_redis_functions(redis=None):
    class Closure:          # Use closure instead of glob to prevent polluting global namespace
        def __init__(self):
            self.channels = {}

        def stop_all(self):
            for key in self.channels:
                self.channels[key].stop()

    closure = Closure()

    def subscribe_redis(key_space="*", event_handler=make_handle_redis_events(), redis=redis):
        if redis is None:                   # Could be remote
            redis = get_redis()

        pubsub = redis.pubsub()
        pubsub.psubscribe(**{f'__keyspace@0__:{key_space}': event_handler})
        thread = pubsub.run_in_thread(sleep_time=0.001)
        closure.channels[key_space] = thread
        return thread

    def get_subscribed_redis() -> Closure:  # Return an obj with list of  all subscribed channels
        return closure

    def unsubscribe_redis(key_space=None):  # Unsubscribe from a keys_pace, or all key_spaces
        try:
            if key_space is None:
                print(f"{Tc.CGREEN}Stopping ALL redis channel {Tc.CEND}")
                keys = dic_keys(closure.channels)
                for key in keys:
                    thread = closure.channels[key]
                    thread.stop()
                    print(f"{Tc.CGREEN}Successfully stopped channel '{key}' {Tc.CEND}")
                    closure.channels.pop(key)
            else:
                thread = closure.channels[key_space]
                thread.stop()
                print(f"{Tc.CGREEN}Successfully stopped channel '{key_space}' {Tc.CEND}")
                closure.channels.pop(key_space)
        except Exception as e: report_error(e, f'unsubscribe_redis(key_space="{key_space}")')
    return subscribe_redis, unsubscribe_redis, get_subscribed_redis

subscribe_redis, unsubscribe_redis, get_subscribed_redis = create_sub_redis_functions()


def mmap(*args):
   return list(map(*args))


def msum(x): return reduce(lambda a, b: a + b, x)


def mmerge(left, right, on, do_return_dics=False):
    dics = {}
    left = left.copy()
    for col in right.columns:
        if col == on: continue
        dics[col] = dic = {k: v for k, v in zip(right[on], right[col])}
        left[col] = left[on].map(lambda x: dic[x] if x in dic else np.NaN)
    if do_return_dics: return left, dics
    return left


def create_threading_func_wrapper(MAX_NUM_THREADS=100):
    threads = []
    def threading_func_wrapper(func, delay=0.001, args=None, start=True, threads=threads):
        import threading
        if args is None:
            func_thread = threading.Timer(delay, func)
        else:
            func_thread = threading.Timer(delay, func, (args,))
        if start: func_thread.start()
        threads.append(func_thread)
        threads = threads[-MAX_NUM_THREADS:]
        return func_thread

    def get_threads(threads=threads):
        return threads

    return threading_func_wrapper, get_threads


def dic_keys(dic):
    assert type(dic) == dict
    return list(dic.keys())


def postRequest(raw, url='', verbosity=0):
    import requests
    try:
        response = requests.post(url, data=json.dumps(raw))
        text = response.text
        if verbosity >= 1:
         print(f"[{dt.now().strftime('%H:%M:%S')}] {Tc.CGREEN}POST Request to {url} succeeded{Tc.ENDC} res = {Tc.CBLUE}{text}{Tc.ENDC}", end="")
        return text
    except Exception as e:
        print(f"[{dt.now().strftime('%H:%M:%S')}] POST Request to {url} failed: {Tc.CREDBG}{e}{Tc.ENDC}")
        return f"POST Request failed: {e}"


def get_latest_vn30_prices():
    from dc_server.redis_tree import REDIS_GLOBAL_TIME
    from sotola import db as db_sotola, get_latest_dp_from_collection
    import numpy as np

    arr = np.array(get_latest_dp_from_collection(db_sotola[f"{REDIS_GLOBAL_TIME.TODAY()}_temp_vn30"])['req'])
    dic_price = {key: value for key, value in zip(arr[:, 0], mmap(float, arr[:, 10]))}
    return dic_price


def getRequest(params=None, url='', verbosity=1):
    if params is None:
        params = {}
    import requests
    try:
        response = requests.get(url=url, params = params)
        data = response.json()
        if verbosity >= 1: print(f"[{dt.now().strftime('%H:%M:%S')}] {Tc.CGREEN}GET Request to {url} succeeded{Tc.ENDC} res = {Tc.CBLUE}{data}{Tc.ENDC}", end="")
        return data
    except Exception as e:
        print(f"[{dt.now().strftime('%H:%M:%S')}] GET Request to {url} failed: {Tc.CREDBG}{e}{Tc.ENDC}")
        return {"msg": f"GET Request failed: {e}"}


def time_format(t, time_only=False):
    st = dt.fromtimestamp(t).strftime('%Y/%m/%d  %I:%M:%S %p')
    if time_only: st = st[-11:-3]
    return st


def isTradingHour(time):
    MARKET_START = "09:00:00"
    MARKET_LUNCH = "11:30:57"
    MARKET_NOON = "13:00:00"
    MARKET_END = "14:45:57"

    return (MARKET_START <= time <= MARKET_LUNCH) or (MARKET_NOON <= time <= MARKET_END)


def get_local_ip():
    import json
    with open("/d/data/config.json", "r") as file:
        config = json.load(file)
    return config['LOCAL_IP']


def report_error(e, function_name="unnamed_foo"):
    from datetime import datetime
    from traceback import print_exc
    print(datetime.now().strftime("%H:%M:%S"), end=" ")
    print(f"GREEN{function_name}()ENDC Có lỗi xảy ra: REDBG{e}ENDC type: REDBG{type(e).__name__}ENDC"
          f"\nArguments:BLUE{e.args}ENDC".replace("REDBG", '\33[41m').
          replace("ENDC", '\033[0m').replace("GREEN", '\33[32m').replace("BLUE", '\33[34m'))
    print_exc()
    print(f"{Tc.CVIOLET2}{dt.now().strftime('%H:%M:%S')} {Tc.CGREENBG2}{Tc.CBLACK}Error ({e}) handled (hopefully), continuing as if nothing happened...{Tc.CEND}")


def list_servers():
    return {
        '35.247.141.201': 'dc',
        '113.161.34.115': 'mx',
        '103.48.194.68': 'lv1',
        '45.119.214.155': 'lv2',
    }


def server_is(name):
    dic = list_servers()
    return dic[get_local_ip()] == name


def execute_cmd(cmd, print_result=True, wait_til_finish=True):
    import subprocess, re, shlex
    cmd = cmd.replace("\n", "").strip().replace("\t", "")
    cmd = cmd.replace("\n", "").strip().replace("\t", "")
    cmd = re.sub(" +", " ", cmd)
    args = shlex.split(cmd)
    process = subprocess.Popen(args, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if not wait_til_finish: return process
    stdout, stderr = process.communicate()
    err = stderr.decode("utf-8")
    if len(err) > 0: print(f"{Tc.CRED}{err}{Tc.CEND}")
    res = stdout.decode("utf-8")
    if print_result: print(f"{Tc.CYELLOW2}{res}{Tc.CEND}")
    return re.sub(" +", " ", res)


def sparse_arr(arr, multiplier=2):
    if type(arr) == list: arr = np.array(arr)
    if multiplier <= 1: return arr
    index = np.array(range(arr.shape[0]))
    filtered_index = np.where((index % multiplier == 0) | (index == len(arr) -1))
    lst = mmap(lambda i: arr[i], filtered_index)[0]
    # if (len(arr) - 1) % multiplier != 0: np.concatenate(lst, arr[-1]])
    return lst


def str_wrap(s):
    if type(s) == type({'a':1}.keys()): s = list(s)
    if type(s) != str: s = str(s)
    return s if len(s) < 70 else s[:60] + " ... " + s[-30:] + ""


def _create_tiktok():
    class Closure: pass
    closure = Closure()
    closure._tik = time()

    def tik():
        closure._tik = time()
        print('%sStarted%s '%(Tc.CBLUE2, Tc.CEND) + f'timing... at {dt.now().strftime("%H:%M:%S")}.')

    def tok():
        print('Elapsed time: %s%.5f%s seconds' % (Tc.CBLUE2, time()-closure._tik, Tc.CEND))

    return tik, tok

tik, tok = _create_tiktok()
bar  = "\x1b[96m" + "="*50 + "\x1b[0m"
bar1 = "\x1b[96m" + "="*50 + "\x1b[93m"
bar2 = "\x1b[96m" + "="*50 + "\x1b[0m"

def exec2(cmd="pwd", wait_til_finish=False): #exec2
    from subprocess import Popen, PIPE
    p = Popen(cmd.split(' '), stdout=PIPE)
    if not wait_til_finish: return p
    arr = p.communicate()
    res = arr[0].decode('utf-8')
    if len(res) >= 1 and res[-1]=="\n": return res[:-1]
    return res


def set_df_display_option(num=6):
    import pandas as pd
    pd.set_option("display.max_columns", num)


def get_plasma(socket="/tmp/plasma"):
    from pyarrow import plasma
    from pyarrow._plasma import PlasmaClient
    client: PlasmaClient = plasma.connect(socket)
    return client


def get_redis(decode_responses=True):
    from redis import StrictRedis
    return StrictRedis(host='localhost', port=6379, decode_responses=decode_responses)


def vals(dic): return list(dic.values())


def get_mongo():
    from pymongo import MongoClient
    return MongoClient("localhost", 27017)


def get_db_sotola():
    from pymongo import MongoClient
    return MongoClient("localhost", 27017)["dc_data"]


def get_db_2018():
    from pymongo import MongoClient
    return MongoClient("localhost", 27018)["dc_test"]


def get_mongo_ver2():
    from pymongo import MongoClient
    return MongoClient("localhost", 27018)


def random_hex():
    import secrets
    return secrets.token_hex(25)


def scary(s):
    return f'{Tc.CREDBG2}{s}{Tc.CEND}'


class Tc:
    HEADER = '\033[95m' # This is a comment line
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    CEND = '\33[0m'
    CBOLD = '\33[1m'
    CITALIC = '\33[3m'
    CURL = '\33[4m'
    CBLINK = '\33[5m'
    CBLINK2 = '\33[6m'
    CSELECTED = '\33[7m'

    CBLACK = '\33[30m'
    CRED = '\33[31m'
    CGREEN = '\33[32m'
    CYELLOW = '\33[33m'
    CBLUE = '\33[34m'
    CVIOLET = '\33[35m'
    CBEIGE = '\33[36m'
    CWHITE = '\33[37m'

    CBLACKBG = '\33[40m'
    CREDBG = '\33[41m'
    CGREENBG = '\33[42m'
    CYELLOWBG = '\33[43m'
    CBLUEBG = '\33[44m'
    CVIOLETBG = '\33[45m'
    CBEIGEBG = '\33[46m'
    CWHITEBG = '\33[47m'

    CGREY = '\33[90m'
    CRED2 = '\33[91m'
    CGREEN2 = '\33[92m'
    CYELLOW2 = '\33[93m'
    CBLUE2 = '\33[94m'
    CVIOLET2 = '\33[95m'
    CBEIGE2 = '\33[96m'
    CWHITE2 = '\33[97m'

    CGREYBG = '\33[100m'
    CREDBG2 = '\33[101m'
    CGREENBG2 = '\33[102m'
    CYELLOWBG2 = '\33[103m'
    CBLUEBG2 = '\33[104m'
    CVIOLETBG2 = '\33[105m'
    CBEIGEBG2 = '\33[106m'
    CWHITEBG2 = '\33[107m'


if __name__ == "__main__":
    print("Use the following code to test import time of lazy core: (0.003s)")
    t = 0
    def startx():
        from time import time
        global t
        t = time()


    def timex():
        from time import time
        global t
        print(time() - t)

    startx()
    timex()


def text_wrap(data, color=Tc.CBEIGE2, length=60):
    s = str(data)
    if len(s) < 100: return f"{color}{s}{Tc.CEND}"
    return f"{color}{s[:length]} {Tc.CEND} ... {color}{s[-20:]}{Tc.CEND}"


def transform_df_to_dic(df): return {key:df[key].values.tolist() for key in df.columns}


def transform_kis_time_vectorized(df, time='time'):
    from datetime import datetime as dt
    return ("1111_11_11" + df[time]).map(lambda x: dt.fromtimestamp(dt.strptime(x, '%Y_%m_%d%H%M%S').timestamp() + 7 * 3600).strftime("%H:%M:%S"))


def transform_kis_time_scalar(time):
    from datetime import datetime as dt
    f = lambda x: dt.fromtimestamp(dt.strptime('1111_11_11' + x, '%Y_%m_%d%H%M%S').timestamp() + 7 * 3600).strftime("%H:%M:%S")
    return f(time)

# noinspection PyUnresolvedReferences
def pprint(data):
    import json
    from pygments import highlight
    from pygments.lexers import JsonLexer
    from pygments.formatters import TerminalFormatter

    if type(data) != str: data = json.dumps(data)
    json_object = json.loads(data)
    json_str = json.dumps(json_object, indent=4, sort_keys=True)
    print(highlight(json_str, JsonLexer(), TerminalFormatter()))


_KIS_DIC_LAZY = {}
def convert_kis_time(t, write_to_redis=False):
    from dc_server.redis_tree import REDIS_TREE
    def initiate():
        def f(n):
            return f"0{n}" if n < 10 else str(n)

        for hour in range(2, 8):
            for minute in range(60):
                for second in range(60):
                    s = f"{f(hour + 7)}:{f(minute)}:{f(second)}"
                    if "09:00:00" <= s <= "11:34:59" or "12:59:00" <= s <= "14:48:59":
                        _KIS_DIC_LAZY[f"{f(hour)}{f(minute)}{f(second)}"] = s
        if write_to_redis:
            from redis import StrictRedis
            r = StrictRedis(decode_responses=False)
            r[REDIS_TREE.UTILITIES.URI_KIS_TIME_LOOKUP] = pickle.dumps(_KIS_DIC_LAZY)

    if len(_KIS_DIC_LAZY) == 0: initiate()
    return _KIS_DIC_LAZY[t]

_REVERSE_KIS_DIC_LAZY = {}
def convert_back_to_kis_time(t, write_to_redis=False):
    if "11:31:00" <= t < "13:00:00": return '043059'
    from dc_server.redis_tree import REDIS_TREE
    def initiate():
        def f(n):
            return f"0{n}" if n < 10 else str(n)

        for hour in range(2, 8):
            for minute in range(60):
                for second in range(60):
                    s = f"{f(hour + 7)}:{f(minute)}:{f(second)}"
                    if "09:00:00" <= s <= "11:34:59" or "12:59:00" <= s <= "14:48:59":
                        _REVERSE_KIS_DIC_LAZY[s] = f"{f(hour)}{f(minute)}{f(second)}"
        if write_to_redis:
            from redis import StrictRedis
            r = StrictRedis(decode_responses=False)
            r[REDIS_TREE.UTILITIES.URI_KIS_TIME_LOOKUP+'.reverse'] = pickle.dumps(_REVERSE_KIS_DIC_LAZY)

    if len(_REVERSE_KIS_DIC_LAZY) == 0: initiate()
    if t not in _REVERSE_KIS_DIC_LAZY: return '074600'
    return _REVERSE_KIS_DIC_LAZY[t]


class _HELPER_FRAME2:
    @staticmethod
    def f(n):
        return f"0{n}" if n < 10 else str(n)
    @staticmethod
    def get_frame():
        count = 0
        index_to_time, time_to_index = {}, {}
        for hour in range(9, 15):
            for minute in range(0, 60):
                for second in range(0, 60):
                    s = f"{_HELPER_FRAME2.f(hour)}:{_HELPER_FRAME2.f(minute)}:{_HELPER_FRAME2.f(second)}"
                    if "09:00:00" <= s <= "11:32:59" or "12:59:00" <= s <= "14:46:59":
                        time_to_index[s] = count
                        index_to_time[count] = s
                        count += 1
        return time_to_index, index_to_time


class FRAME2:
    TIME_TO_INDEX, INDEX_TO_TIME = {}, {}

    @staticmethod
    def times():
        if len(FRAME2.TIME_TO_INDEX) == 0:
            FRAME2.TIME_TO_INDEX, FRAME2.INDEX_TO_TIME = _HELPER_FRAME2.get_frame()
        return list(FRAME2.TIME_TO_INDEX.keys())

    @staticmethod
    def time_to_index(t):
        if len(FRAME2.TIME_TO_INDEX) == 0:
            FRAME2.TIME_TO_INDEX, FRAME2.INDEX_TO_TIME = _HELPER_FRAME2.get_frame()
        return FRAME2.TIME_TO_INDEX[t]


    @staticmethod
    def index_to_time(i):
        if len(FRAME2.TIME_TO_INDEX) == 0:
            FRAME2.TIME_TO_INDEX, FRAME2.INDEX_TO_TIME = _HELPER_FRAME2.get_frame()
        return FRAME2.INDEX_TO_TIME[i]


    @staticmethod
    def sparsify(df, return_type="df"):
        assert return_type in ["arr", "df"]
        df = copy(df).reindex(FRAME2.times)
        df["last"] = df["last"].fillna(method="bfill").fillna(method="ffill")
        df[["matchingSumBU", "matchingSumSD", "netBUSD"]] = df[["matchingSumBU", "matchingSumSD", "netBUSD"]].fillna(method="ffill").fillna(0)
        return df if return_type == "df" else df.to_numpy()


def erase_memory():
    import sys
    sys.modules[__name__].__dict__.clear()


def get_host_name(GENERATING=None):
    if not GENERATING is None:
        with open("/d/data/host_info/host_name", "w") as file: file.write(GENERATING)
    with open("/d/data/host_info/host_name", "r") as file:
        return file.read()


def checkout_redis_lists(pattern="*", day=None):
    from dc_server.redis_tree import REDIS_GLOBAL_TIME
    r = lazy_get_redis(decode_responses=True)
    if day is None: day = REDIS_GLOBAL_TIME.TODAY()
    lst = sorted([x for x in r.keys() if 'csv' in x and day in x and (pattern=="*" or pattern.lower() in x.lower())])
    return lst


def lazy_get_redis(*args, decode_responses=True, host="localhost", port=6379, **kwargs):
    from dc_server.redis_tree import get_redis
    return get_redis(*args, decode_responses=decode_responses, host="localhost", port=6379, **kwargs)


def grab_vn30_ref_prices(DAY=None):
    from dc_server import REDIS_GLOBAL_TIME
    class REDIS_KEYS:
        class BACKTEST_03:
            KEY1_PS_BUSD = 'misc.backtest_03.ps_busd'
            KEY2_VN30_REF_prices = 'misc.backtest_03.vn30_ref_prices'

    def maybe_update(DAY):
        from pymongo import MongoClient
        client = MongoClient('113.161.34.115', 27017)
        db_sotola = client["dc_data"]
        rb = lazy_get_redis(decode_responses=False)
        dic_vn30, dic_ref_prices = pickle.loads(rb[REDIS_KEYS.BACKTEST_03.KEY2_VN30_REF_prices])
        if DAY in dic_ref_prices: return dic_vn30, dic_ref_prices
        new_days = [x[:10] for x in sorted(db_sotola.list_collection_names()) if
                    '_temp_vn30' in x and x[:10] not in dic_ref_prices]
        for new_day in new_days:
            try:
                col = db_sotola[f'{new_day}_temp_vn30']
                data = np.array(col.find_one()['req'])
                dic = {key[:3]: float(value) for key, value in zip(data[:, 0], data[:, 1])}
                dic_vn30[new_day] = list(dic.keys())
                dic_ref_prices[new_day] = dic
            except Exception as e:
                report_error(e, f'Dung temp_vn30 de update vn30_ref_price {new_day}')
        rb[REDIS_KEYS.BACKTEST_03.KEY2_VN30_REF_prices] = pickle.dumps((dic_vn30, dic_ref_prices))
        return dic_vn30, dic_ref_prices

    if DAY is None: DAY = REDIS_GLOBAL_TIME.TODAY()
    dic_vn30, dic_ref_prices = maybe_update(DAY)
    keys = list(dic_vn30.keys())
    values = vals(dic_ref_prices)
    if DAY < keys[0]: return dic_ref_prices[keys[0]]
    if DAY > keys[-1]: return dic_ref_prices[keys[-1]]
    for i in range(len(keys)):
        j = len(keys) - 1 - i
        key = keys[j]
        if key <= DAY: return dic_ref_prices[key]


def sort_dic(dic, reverse=False):
    return {k: v for k, v in sorted(dic.items(), key=lambda item: item[1], reverse=reverse)}


def down_sample(df, field='x', STEP = 15):
    df = copy(df)
    start = int(df.iloc[0][field] / STEP) * STEP + STEP
    end   = int(df.iloc[-1][field] / STEP + 1) * STEP
    indices = list(range(start, end, STEP))
    if df.iloc[0][field] < start: indices.insert(0, df.iloc[0][field])
    if df.iloc[-1][field] >  end - STEP: indices.append(df.iloc[-1][field])
    df = df.set_index(field).reindex(indices).fillna(method='ffill')
    return df


def compute_downsized_index(df, col='x', STEP = 15):
    start = int(df.iloc[0][col] / STEP) * STEP + STEP
    end   = int(df.iloc[-1][col] / STEP + 1) * STEP
    indices = list(range(start, end, STEP))
    if df.iloc[0][col] < start: indices.insert(0, df.iloc[0][col])
    if df.iloc[-1][col] >  end - STEP: indices.append(df.iloc[-1][col])
    return indices


def get_plasma_client2(db=0):
    from dc_server.plasma.basic_store import Store
    store = Store(key='nah', db=db)
    from pyarrow.plasma import PlasmaClient
    client: PlasmaClient = store.client
    return client


def get_plasma_client(socket='/tmp/plasma', db=0):
    from pyarrow import plasma
    from pyarrow.plasma import PlasmaClient

    if db != 0:
        socket = socket + str(db)
    client: PlasmaClient =  plasma.connect(socket)
    return client


def read_unflatten_csv(file_path='/home/ubuntu/Desktop/VN30.csv'):
    import pandas as pd
    with open(file_path, 'r') as file:
        lines = file.readlines()

    columns = lines[0].replace('\n', '').split(',')
    lst_datapoints = []

    for i in range(1, len(lines)):
        line = lines[i].replace('\n', '').split(',')
        start_index = [i for i in range(len(line)) if '[' in line[i]][0]
        end_index   = [i for i in range(len(line)) if ']' in line[i]][0] + 1
        nested_content = json.loads(', '.join(line[start_index:end_index]).replace("'", '"'))
        flatten = {key: value for key, value in zip(columns[:start_index], line[:start_index])}
        flatten[columns[start_index]] = nested_content
        for key, value in zip(columns[start_index+2:], line[end_index+1:]): flatten[key] = value
        lst_datapoints.append(flatten)

    df_vn30_index_official = pd.DataFrame(lst_datapoints)
    return df_vn30_index_official


def grab_today_vn30_stock_csv(DAY=None, host="localhost"):
    from redis import StrictRedis
    from dc_server.redis_tree import REDIS_GLOBAL_TIME

    def csv_to_stock(csv): return csv.split('/')[-1][:3]
    r = StrictRedis(host=host, decode_responses=True)
    if DAY is None: DAY = REDIS_GLOBAL_TIME.TODAY()
    VN30_LIST = list(grab_vn30_ref_prices().keys())
    csv_list = [x for x in r.keys() if DAY in x and '.csv' in x and 'StockAutoItem' in x and 'Bid' not in x]
    csv_list = [x for x in csv_list if csv_to_stock(x) in VN30_LIST]
    CSV_TO_STOCK = {x: csv_to_stock(x) for x in csv_list}
    STOCK_TO_CSV = {value: key for key, value in CSV_TO_STOCK.items()}
    return csv_list, CSV_TO_STOCK, STOCK_TO_CSV


class CURATED_PLASMA:
    URI = "plasma.indicators.curated"
    URI_DATATABLE = URI + '.data_table'


def overwrite_pget(data, dataset, day, db=0):
    from redis import StrictRedis
    pclient = get_plasma_client(0)
    oid = pclient.put(data)
    rb = StrictRedis()
    rb[f'{CURATED_PLASMA.URI}.{dataset}.{day}'] = oid.binary()
    buffer = pclient.get_buffers([oid])
    return oid, buffer


def gen_pget(db=0):
    from pymongo import MongoClient
    import pandas as pd
    from redis import StrictRedis

    def pget(dataset, day, return_oid=False):
        def pget_helper(key):
            return pclient.get(key, timeout_ms=10)
        from pyarrow._plasma import ObjectID
        uri = f"{CURATED_PLASMA.URI}.{dataset}.{day}"
        if not return_oid: return pget_helper(ObjectID(rb[uri]))
        return ObjectID(rb[uri])

    def grab_curated_data_table():
        df = pd.DataFrame(json.loads(StrictRedis(decode_responses=True)[CURATED_PLASMA.URI_DATATABLE]))[['oid', 'uri',  'day', 'dataset']].set_index('day')
        df = df[df.index.map(lambda x: dt.strptime(x, '%Y_%m_%d').weekday() < 5)]
        return df

    pclient = get_plasma_client(db=db)
    client_ws = MongoClient("ws", 27022)
    db_ws = client_ws["curated"]
    rb = StrictRedis()

    # coll_names = sorted(db_ws.list_collection_names(), reverse=True)
    # lst_datasets = []
    return pget, grab_curated_data_table


def n_tradingdays_from_day(start=None, ndays=0):
    if start is None: start = dt.now().strftime('%Y_%m_%d')
    import pandas as pd, numpy as np
    tet_holidays = ['2021_02_10', '2021_02_11', '2021_02_12', '2021_02_15', '2021_02_16',
                    '2022_01_31', '2022_02_01', '2022_02_02', '2022_02_03', '2022_02_04']
    gio_to = ['2021_04_21', '2022_04_11']
    bamuoi_motnam = ['2021_04_30', '2021_05_03', '2022_05_02', '2022_05_03']
    quoc_khanh = ['2020_09_02', '2021_09_02', '2021_09_03', '2022_09_01', '2022_09_02', '2023_09_02']
    nam_moi = ['2022_01_03']
    holiday = [        
        '2024_01_01',
        '2024_02_08',
        '2024_02_09',
        '2024_02_10',
        '2024_02_11',
        '2024_02_12',
        '2024_02_13',
        '2024_02_14',
        '2024_04_18',
        '2024_04_29',
        '2024_04_30',
        '2024_05_01',
        '2024_01_14',
        '2024_09_02',
        '2024_09_03',
        '2025_01_01',
    ]
    ngay_le = tet_holidays + quoc_khanh + gio_to + bamuoi_motnam + nam_moi + holiday

    start = start.replace('_', '-')

    if ndays == 0: # truong hop khong chon ngay thi
        # neu hom nay la ngay cuoi tuan hoac ngay le thi lay ngay trading cuoi cung, con k thi lay ngay hien tai
        if (not (bool(len(pd.bdate_range(start, start))))) | (start.replace('-', '_') in ngay_le):
            ndays = -1
        else:
            ndays = -2
        end = (pd.to_datetime(start, format='%Y-%m-%d') + pd.Timedelta(f'{ndays + 30 * ndays / abs(ndays)}d')).strftime(
            '%Y-%m-%d')
        business_day_pandas = pd.bdate_range(start=end, end=start).strftime('%Y_%m_%d').values
        the_one_day = np.setdiff1d(business_day_pandas, ngay_le)[ndays]
        return the_one_day
    else:
        end = (pd.to_datetime(start, format='%Y-%m-%d') + pd.Timedelta(f'{ndays + 30 * ndays / abs(ndays)}d')).strftime(
            '%Y-%m-%d')
        if start < end:
            business_day_pandas = pd.bdate_range(start=start, end=end).strftime('%Y_%m_%d').values
            trading_days = np.setdiff1d(business_day_pandas, ngay_le)[:ndays]
        else:
            business_day_pandas = pd.bdate_range(start=end, end=start).strftime('%Y_%m_%d').values
            trading_days = np.setdiff1d(business_day_pandas, ngay_le)[ndays:]
        return trading_days


def maybe_create_dir(DIR, verbosity=1):
    import os
    if not os.path.isdir(DIR):
        execute_cmd(f"mkdir -p {DIR}")
        if verbosity >= 1: print(f"Created folder {DIR}")


def get_pythons(verbosity = 1):
    import psutil
    import os

    process_is_running = False
    lst = []
    for process in psutil.process_iter():
        if 'python' in process.cmdline():
            pid = process.pid
            script = psutil.Process(pid).cmdline()[1]
            folder = os.path.abspath(script)
            lst.append([pid, folder, process.cmdline()])
            if verbosity >= 1: print(lst[-1])
    return lst


def  gen_plasma_functions(db=0):
    import warnings
    warnings.filterwarnings('ignore', category=DeprecationWarning)
    warnings.filterwarnings('ignore', category=FutureWarning)
    from redis import StrictRedis

    host_name_sleep = 'unknown'
    try:
        host_name_sleep = get_host_name()
    except Exception as e:
        if len(str(e)) == 0: print(e, end='')


    class Closure:
        pclient = get_plasma_client(db=db)
        rb = StrictRedis()

    def time_stamp():
        return str.encode(str(int(dt.now().timestamp() * 1000_000)))

    def save_to_plasma(data):
        oid = Closure.pclient.put(data)
        buffer = Closure.pclient.get_buffers([oid])[0]

        return oid, buffer

    def load_from_plasma(oid):
        # noinspection PyArgumentList
        return Closure.pclient.get(oid, timeout_ms=10)

    def pload(key, return_pid=False, return_all=False, return_time=False):
        from pyarrow._plasma import ObjectID, ObjectNotAvailable
        import os


        binary_key = Closure.rb[key][:20]
        plasma_key = ObjectID(binary_key)
        message = f'debug.plasma.{db}.read {dt.now()} key={key} writer_pid={int(Closure.rb[key][36:])} reader_pid={os.getpid()} '
        # if host_name_sleep == 'lv2': sleep(0.025)
        # elif host_name_sleep == 'ws': sleep(0.05)

        obj = load_from_plasma(plasma_key)
        if type(obj) == type:
            print(f"{dt.now().strftime('%H:%M:%S')}: {Tc.CBLUEBG2}{Tc.CBLACK}{key}{Tc.CEND}", "plasma key not found")
            Closure.rb.rpush(f'debug.plasma.{db}.read', message + 'FAILED')
            sleep(0.05)
            return ObjectNotAvailable
        
        # try:
        #     # obj = Closure.pclient.get(plasma_key, timeout_ms=10)
        #     obj = load_from_plasma(plasma_key)
        # except ObjectNotAvailable:
        #     print(f"{dt.now().strftime('%H:%M:%S')}: {Tc.CBLUEBG2}{Tc.CBLACK}{key}{Tc.CEND}", "plasma key not found")
        #     Closure.rb.rpush(f'debug.plasma.{db}.read', message + 'FAILED')
        #     sleep(0.05)
        #     return ObjectNotAvailable

        Closure.rb.rpush(f'debug.plasma.{db}.read', message + 'SUCCESS')
        res = [obj]

        if return_all:
            import psutil
            pid = int(Closure.rb[key][36:])
            x = psutil.Process(pid)
            return obj, x.cmdline(), x.pid
        if return_pid: res.append(int(Closure.rb[key][36:]))
        if return_time:
            t = int(Closure.rb[key][20:36].decode())
            t = dt.fromtimestamp(t / 1000_000).strftime('%Y_%m_%d %H:%M:%S.%f')
            res.append(t)
        return res[0] if len(res) == 1 else res

    def psave(key, data):
        start_time = time()
        t = dt.now()
        Closure.rb.rpush(f'debug.plasma.{db}', f'{t.timestamp()}  key={key} started ')
        import os
        oid = Closure.pclient.put(data)
        buffer = Closure.pclient.get_buffers([oid])[0]
        Closure.rb[key] = oid.binary() + time_stamp() + f'{os.getpid()}'.encode()
        t = dt.now()

        try:
            buffer = Closure.pclient.get_buffers([oid], timeout_ms=10)[0]
            size = buffer.size  / 1048576
            #buffer = Closure.pclient.get_metadata([oid], timeout_ms=10)[0]
        except Exception as e:
            report_error(e)
            size = 0

        Closure.rb.rpush(f'debug.plasma.{db}', f'db={db} '
                         f'time={t.strftime("%Y_%m_%d %H:%M:%S .%f")} '
                         f'{t.timestamp()} '
                         f'pid={os.getpid()} key={key} '
                         f'took={time() - start_time:.5f} '
                         f'size={size}'.encode())
        return oid, buffer

    def disconnect():
        Closure.pclient.disconnect()

    return save_to_plasma, disconnect, psave, pload


def summarize_plasma(db=0):
    import pandas as pd

    client = get_plasma_client(db=db)
    lst = vals(client.list())
    # lst = [x for x in lst if x['ref_count'] > 0]

    df = pd.DataFrame(lst)
    print('Plasma objects with ref:', df['data_size'].sum()/1000_000_000, 'GB')
    return df


class REDIS_PLASMA_KEY:
    CURRENT_F1 = 'plasma.current.f1'
    CURRENT_PREMIUM = 'plasma.current.premium'
    CURRENT_DFS = 'plasma.current.dfs'
    CURRENT_BUSD_ACC = 'plasma.current.busd_accumulated' # 8
    CURRENT_BUSD = 'plasma.current.busd'
    CURRENT_ARBIT_RAW = 'plasma.current.arbit_raw'
    CURRENT_ARBIT_SUMMARY = 'plasma.current.arbit_summary'
    CURRENT_OLD_ARBIT = 'plasma.current.old_arbit'
    CURRENT_NEW_BUSD = 'plasma.current.new_busd' # 9
    CURRENT_VN50 = 'plasma.current.new_busd' # 10
    CURRENT_FIINPRO_F1 = 'plasma.current.fiinpro.f1' #11


def grab_kis_from_mx_db(DAY:""):
    from pymongo import MongoClient
    import pandas as pd
    if len(DAY) == 0:
        from dc_server.redis_tree import REDIS_GLOBAL_TIME
        DAY = REDIS_GLOBAL_TIME.LAST_TRADING_DAY()
    client = MongoClient("mx")
    coll = client["dc_data"][f'{DAY}_KIS']
    assert coll.count_documents({}) >= 30
    VN30 = list(grab_vn30_ref_prices().keys())
    dfs = {}
    for stock in VN30:
        df = pd.DataFrame(coll.find_one({"desc": f"KIS {stock}"})['data'])
        if df.iloc[0]['time'] > df.iloc[-1]['time']: df = df.iloc[::-1]
        dfs[stock] = df
        print(len(df), end = " ")
    print()
    return dfs


def set_output_file(fn):
    from bokeh.plotting import output_file
    import os
    DIR = '/'.join(fn.split('/')[:-1])
    if not os.path.isdir(DIR): execute_cmd(f"mkdir -p {DIR}")
    output_file(fn)


def plasma_obj_size(key):
    from pyarrow._plasma import ObjectID
    from redis import StrictRedis
    pclient = get_plasma_client()
    rb = StrictRedis('localhost', decode_responses=False)
    binary_key = rb[key][:20]
    plasma_key = ObjectID(binary_key)
    size_mb = round(pclient.list()[plasma_key]['data_size']/(1024*1024), 2)
    return size_mb


def grab_kis(DAY, verbosity=0):
    from pymongo import MongoClient
    import pandas as pd
    for port in [27017, 27019]:
        try:
            client = MongoClient('mx', port)
            VN30_LIST = list(grab_vn30_ref_prices().keys())
            col = client["dc_data"][f'{DAY}_KIS']
            dfs = {}
            for stock in VN30_LIST:
                df = pd.DataFrame(col.find_one({'desc': f'KIS {stock}'})['data'])
                dfs[stock] = df.iloc[::-1]
            if verbosity >= 1: print(f'found data in mx:{port}')
            return dfs
        except Exception as e: print(scary(e) + '   ' + scary(port), f'grab_kis{DAY}')
    return None


def reset_index(df):
    columns = df.columns
    dff = df.reset_index()[columns]
    return dff


def sort_values(df, values, columns=None):
    if columns is None: columns = df.columns
    df = copy(df.sort_values(values))
    df = df.reset_index()[columns]
    return df


def compare_dfs(df_old, df_new):
    df_diff = df_old - df_new
    columns = df_diff.columns
    df = df_diff.apply(lambda row: reduce(lambda x, y: x + y, map(lambda col: abs(row[col]), columns)), axis=1)
    return df


class Cs:
    DIC_WEIGHTS_2021_07 = {'BID': 0.61, 'BVH': 0.38, 'CTG': 2.97, 'FPT': 4.82, 'GAS': 0.74,
                           'HDB': 3.02, 'HPG': 10.19, 'KDH': 1.11, 'MBB': 4.95, 'MSN': 3.76,
                           'MWG': 3.83, 'NVL': 4.34, 'PDR': 1.22, 'PLX': 0.77, 'PNJ': 1.54,
                           'POW': 0.52, 'REE': 0.69, 'SBT': 0.39, 'SSI': 1.2, 'STB': 3.81,
                           'TCB': 9.66,'TCH': 0.41, 'TPB': 1.73, 'VCB': 3.6, 'VHM': 4.39,
                           'VIC': 7.24, 'VJC': 2.91, 'VNM': 8.6, 'VPB': 9.07, 'VRE': 1.53}
    DIC_WEIGHTS = {'ACB': 6.38, 'BID': 0.51, 'BVH': 0.29, 'CTG': 1.9, 'FPT': 5.57, 'GAS': 0.66,
                   'GVR': 0.4099999999999999, 'HDB': 2.89, 'HPG': 8.94, 'KDH': 1.27, 'MBB': 4.61,
                   'MSN': 4.25, 'MWG': 3.8900000000000006, 'NVL': 4.1, 'PDR': 1.38, 'PLX': 0.64,
                   'PNJ': 1.34, 'POW': 0.38, 'SAB': 0.86, 'SSI': 1.79, 'STB': 3.94, 'TCB': 8.94,
                   'TPB': 1.73, 'VCB': 3.07, 'VHM': 5.5, 'VIC': 7.84, 'VJC': 2.35, 'VNM': 5.53,
                   'VPB': 7.48, 'VRE': 1.55}
    DIC_PRICES  = {'ACB': 36.2, 'BID': 41.1, 'BVH': 51.1, 'CTG': 34.4, 'FPT': 94.0, 'GAS': 89.6,
                   'GVR': 33.7, 'HDB': 33.7, 'HPG': 47.3, 'KDH': 40.7, 'MBB': 28.9, 'MSN': 134.0,
                   'MWG': 164.1, 'NVL': 104.0, 'PDR': 92.5, 'PLX': 51.3, 'PNJ': 95.8, 'POW': 10.7,
                   'SAB': 159.0, 'SSI': 54.6, 'STB': 29.9, 'TCB': 51.1, 'TPB': 35.0, 'VCB': 98.0,
                   'VHM': 108.3, 'VIC': 107.2, 'VJC': 113.0, 'VNM': 86.1, 'VPB': 61.0, 'VRE': 27.7}
    VN30_LIST = list(DIC_WEIGHTS.keys())


class PADAPTERS:
    load_current_busd_acc, \
    load_premium, \
    load_dfs, \
    load_busd, \
    load_busd_sparsified, \
    load_arbit_raw, \
    load_arbit_final, \
    load_f1, \
    load_ps, \
    load_ps_ssi_ws, save_ps_ssi_ws, \
    load_dfs_ssi_ws, save_dfs_ssi_ws, \
    load_profiles_hpg, save_profiles_hpg,\
    load_f1_fiinpro \
        = [lambda x:x] * 16
    load_ssi_ws_hose500 = lambda x: x
    pload = lambda x:x
    initiated = False
    pload1 = lambda x: x
    save1 = lambda x, y: x

    @staticmethod
    def init_ssi_ws():
        try:
            from dc_server.ssi_ws.ssi_ws_stock_parser import BASE_PARSER, PARSER
            import pandas as pd
            _, _, PADAPTERS.psave1, PADAPTERS.pload1 = gen_plasma_functions(db=SSI_WS_PLASMA_DB)
            def get_ssi_ws_hose500():
                cube = PADAPTERS.pload1(PARSER.plasma_output_key())
                return pd.DataFrame(cube, columns=BASE_PARSER.NUMERICAL_COLUMNS)
            return get_ssi_ws_hose500
        except Exception as e:
            report_error(e, 'PADAPTERS.init_ssi_ws()')

    @staticmethod
    def maybe_initiate():
        from dc_server.kis_worker_group import kis_connectors
        from dc_server.redis_tree import REDIS_GLOBAL_TIME
        if not PADAPTERS.initiated:
            PADAPTERS.initiated = True
            PADAPTERS.load_premium = kis_connectors.KIS_Redis_Connectors.KIS_AGGREGATOR_WORKER.load_kis_premium_from_plasma
            PADAPTERS.load_dfs = kis_connectors.KIS_Redis_Connectors.KIS_AGGREGATOR_WORKER.load_kis_dfs_from_plasma
            PADAPTERS.load_busd = kis_connectors.KIS_Redis_Connectors.KIS_BUSD.load_kis_busd_from_plasma
            PADAPTERS.load_busd_sparsified = kis_connectors.KIS_Redis_Connectors.KIS_BUSD.load_busd_sparsified_from_plasma
            PADAPTERS.load_arbit_raw = kis_connectors.KIS_Redis_Connectors.KIS_ARBIT_PREP_WORKER.load_arbit_raw_from_plasma
            PADAPTERS.load_arbit_final = kis_connectors.KIS_Redis_Connectors.KIS_ARBIT_PLOT_WORKER.load_arbit_final_from_plasma
            _, _, psave, pload = gen_plasma_functions()
            PADAPTERS.pload = pload
            PADAPTERS.load_f1 = lambda: PADAPTERS.pload(f"plasma.indicators.kis_ps.f1.{REDIS_GLOBAL_TIME.LAST_TRADING_DAY()}")
            PADAPTERS.load_current_busd_acc = lambda: PADAPTERS.pload(REDIS_PLASMA_KEY.CURRENT_BUSD_ACC)
            PADAPTERS.load_ps = lambda: PADAPTERS.pload(f'plasma.scraper.ps.{REDIS_GLOBAL_TIME.TODAY()}')
            PADAPTERS.load_f1_fiinpro = lambda: PADAPTERS.pload(f'{FIINPRO.F1_PLASMA_KEY}.{REDIS_GLOBAL_TIME.TODAY()}')

            PADAPTERS.save_ps_ssi_ws = lambda df: psave('plasma.current.ps.ssi.ws', df)
            def load_ps_ssi_ws(filter_volume=True):
                df = pload('plasma.current.ps.ssi.ws')
                # noinspection PyTypeChecker
                return df[df['matchVolume'] > 0] if filter_volume else df

            PADAPTERS.load_ps_ssi_ws = load_ps_ssi_ws

            PADAPTERS.save_dfs_ssi_ws = lambda dfs: psave('plasma.current.dfs.ssi.ws', dfs)
            PADAPTERS.load_dfs_ssi_ws = lambda: pload('plasma.current.dfs.ssi.ws')
            PADAPTERS.load_profiles_hpg = lambda: pload('plasma.master.profiles.hpg')
            PADAPTERS.save_profiles_hpg = lambda df: psave('plasma.master.profiles.hpg', df)
            PADAPTERS.load_ssi_ws_hose500 = PADAPTERS.init_ssi_ws()


class NEW:
    @staticmethod
    def get_vn30_daily_data(symbol='VN30',start=None, end=None):
        from dc_server import REDIS_GLOBAL_TIME
        import pandas as pd
        if end is None: end = REDIS_GLOBAL_TIME.TODAY()
        import requests
        if start is None: start = '2019_01_01'
        headers = {
            'Connection': 'keep-alive',
            'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
            'Accept': 'application/json, text/plain, */*',
            'sec-ch-ua-mobile': '?0',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
            'Origin': 'https://dchart.vndirect.com.vn',
            'Sec-Fetch-Site': 'same-site',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Dest': 'empty',
            'Referer': 'https://dchart.vndirect.com.vn/',
            'Accept-Language': 'en-US,en;q=0.9',
        }

        params = (
            ('resolution', 'D'),
            ('symbol', symbol),
            ('from', str(int(dt.strptime(start, '%Y_%m_%d').timestamp()))),
            ('to', str(int(dt.strptime(end, '%Y_%m_%d').timestamp()))),
        )

        response = requests.get('https://dchart-api.vndirect.com.vn/dchart/history', headers=headers, params=params)

        data = response.json()
        df = pd.DataFrame(data)
        df.columns=['time', 'close', 'open', 'high', 'low', 'volume', 'status']
        df['time'] = df['time'].map(lambda x: dt.fromtimestamp(x).strftime('%Y_%m_%d'))

        return df

    @staticmethod
    def compute_atr(_df):
        import pandas as pd
        high_low = _df['high'] - _df['low']
        high_close = np.abs(_df['high'] - _df['close'].shift())
        low_close = np.abs(_df['low'] - _df['close'].shift())
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = np.max(ranges, axis=1)
        atr = true_range.rolling(14).sum()/14
        return atr

    @staticmethod
    def compute_df_ranking(SYMBOL = 'VN30', WINDOW = 20, closes=None):
        import pandas as pd
        _df = NEW.get_vn30_daily_data(SYMBOL)
        if closes is not None: _df["close"] = closes
        _df['change'] = 100* _df['close'].diff(1)/_df['close'].shift(1, fill_value=0)
        _df['volume'] = _df['volume']/1000_000
        _df['mean volume'] = _df['volume'].rolling(WINDOW).mean()
        _df['std volume'] = _df['volume'].rolling(WINDOW).std()
        _df['deviation(std)'] = (_df['volume'] - _df['mean volume']) / _df['std volume']
        _df['atr'] = NEW.compute_atr(_df)
        _df[f'rank({WINDOW})'] = (_df['volume'].rolling(WINDOW).apply(lambda x: pd.Series(x).rank().values[-1])/WINDOW * 100).map(lambda x: f'top {100-x}%')
        _df[f'rank({200})'] = (_df['volume'].rolling(200).apply(lambda x: pd.Series(x).rank().values[-1])/200* 100).map(lambda x: f'top {100-x}%')
        df = copy(_df[_df['change'] < -3])[['time', 'volume', 'deviation(std)', f'rank({WINDOW})', f'rank(200)']] # ['time', 'close', 'open', 'high', 'low', 'volume', 'status', 'change', 'deviation(std)', 'atr', 'rank(20)']
        df.columns = [df.columns[0]] + mmap(lambda x: f'{x}({SYMBOL})', df.columns[1:])
        print(df)
        return _df, df

    @staticmethod
    def test_compute_daily_data():
        import pandas as pd
        _df_vn30, df_vn30 = NEW.compute_df_ranking('VN30')
        closes = _df_vn30['close'].to_list()

        _, df_index = NEW.compute_df_ranking('VNINDEX', closes=closes)
        _, df_ps = NEW.compute_df_ranking('VN30F1M', closes=closes)

        df_merged = pd.concat([df_vn30, df_index[df_index.columns[1:]], df_ps[df_ps.columns[1:]]], axis=1)
        df_merged.to_csv('/home/ubuntu/Desktop/tmp/three_plus_all.csv')


class FIINPRO:
    F1_REDIS_KEY = 'redis_tree.inputs.fiinpro.derivative'
    F1_PLASMA_KEY = 'plasma.fiinpro.f1'


class LAZY_CONFIG:
    @staticmethod
    def determine_fiinpro_redis_source():
        host = get_host_name()
        if host in ['mx', 'ws']: return 'mx'
        return 'lv2'

    @staticmethod
    def determine_f1_f2(day=None):
        if day is None: day = dt.now().strftime('%Y_%m_%d')
        if '2021_08_19' < day <= '2021_09_16':
            return 'VN30F2110', 'VN30F2110'

        return 'error: Unimplemented', 'error: Unimplemented'


def show_ram_usage_mb():
    import os
    import psutil
    pid = os.getpid()
    python_process = psutil.Process(pid)
    memoryUse = round(python_process.memory_info()[0] / 2. ** 20, 2) # memory use in GB
    print(f'{Tc.CYELLOW2}memory usesage: {memoryUse}{Tc.CEND}')


class REDIS_LIST_KEYS:
    SSI_WEBSOCKET_PS = 'redis_lists.input.websocket.ssi.ps'         # '.{DAY}'
    SSI_WEBSOCKET_PS_PLASMA = 'plasma.ws.ssi.ps.current'            # no day
    SSI_HOSE500_RECEIVED = 'redis_lists.node.js.hose500.received'
    SSI_HOSE500_NUMPY = 'redis_lists.node.js.hose500.numpy'
    SSI_HOSE500_SENT = 'redis_lists.node.js.hose500.sent'
    PLASMA_SSI_HOSE500_NUMPY =''

REDIS_LIST_KEYS.PLASMA_SSI_HOSE500_NUMPY = REDIS_LIST_KEYS.SSI_HOSE500_NUMPY.replace('redis_lists', 'plasma')

def inspect_pkey(key, db=0):
    import psutil
    from pyarrow._plasma import ObjectID
    from redis import StrictRedis
    _, _, psave, pload = gen_plasma_functions(db=db)
    v, pid, t = pload(key, return_time=True, return_pid=True)
    procs = [x for x in psutil.process_iter() if pid==x.pid]
    if len(procs) == 0:
        proc = 'process not running'
    else:
        proc = str(procs[0]) + '\n         ' + ' '.join(procs[0].cmdline())
    pclient = get_plasma_client(db=db)
    rb = StrictRedis('localhost', decode_responses=False)
    obj = pclient.list()[ObjectID(rb[key][:20])]
    size = obj['data_size'] / 1048576
    print(f'Data     : {str_wrap(v)}\n'
          f'Size(mb) : {size:.2f} mb\n'
          f'Ref count: {obj["ref_count"]}\n'
          f'Process  : {proc}\n'
          f'Time     : {t}\n')
    return obj


def plasma_bulk_report(lst):
    for key in lst:
        print(f'{Tc.CYELLOW2}{key}{Tc.CEND}:')
        try:
            o = inspect_pkey(key)
        except Exception as e:
            print(key, scary(e), end='\n')


def plasma_bulk_repin(lst, db=0):
    from pyarrow._plasma import ObjectID
    from redis import StrictRedis
    rb = StrictRedis('localhost', decode_responses=False)
    pclient = get_plasma_client(db=db)
    dic = {}
    for key in lst:
        print(f'{Tc.CYELLOW2}{key}{Tc.CEND}:')
        try:
            buffer = None
            buffer = pclient.get_buffers([ObjectID(rb[key][:20])], timeout_ms=10)
            print(buffer)
        except Exception as e:
            print(key, e)
        finally:
            dic[key] = buffer
    rb.close()
    return dic


class LIQUIDITY_PLASMA_KEY:
    BUSD = 'plasma.liquidity.busd'
    TEMP_VN30 = 'plasma.liquidity.temp_vn30'
    TICK_DATA = 'plasma.liquidity.tickdata'
    MY_VN30 = 'plasma.liquidity.my_vn30'
    WEIGHTS = 'plasma.liquidity.weights'
    BIG = 'plasma.liquidity.big'


class MASTER_PLASMA_KEYS:
    HUGE  = 'plasma.huge'
    HUGE_STR = 'plasma.huge.str_columns'
    HUGE_NUMERICAL = 'plasma.huge.numerical'
    HPG_OWNERSHIP_OLD = 'plasma.master.hpg1'  # Not loaded by default
    HPG_BASE = 'plasma.master.hpg2'
    TCB_OWNERSHIP = 'plasma.master.tcb'
    STB_OWNERSHIP = 'plasma.master.stb'
    HPG_PROFILES = 'plasma.master.hpg.profile'
    MAX_OWNERSHIP_NUMERICAL = 'plasma.master.hpg.maxOwnership'

    VPS_TOP100 = 'plasma.master.vps.top100'
    HPG_OWNERSHIP_THROUGH_TIME = 'plasma.master.hpg.ownership.numerical'
    HPG_TICK_DATA = 'plasma.tickdata.hpg'
    HPG_OWNERSHIP_THROUGH_TIME_NUMERICAL = 'plasma.master.hpg.ownership.numerical'
    HPG_OWNERSHIP_LOOKUP_NUMERICAL = 'plasma.master.hpg.lookup.numerical'
    HPG_PROFILES_NUMERICAL = 'plasma.master.hpg.profiles.numerical'

    @staticmethod
    def get_keys_dic():
        return {key: MASTER_PLASMA_KEYS.__dict__[key]
                for key in MASTER_PLASMA_KEYS.__dict__
                if key[:2] != '__'
                and type(MASTER_PLASMA_KEYS.__dict__[key]) == str}

    @staticmethod
    def repin():
        from redis import StrictRedis
        from pyarrow._plasma import ObjectID

        dic = MASTER_PLASMA_KEYS.get_keys_dic()
        pclient = get_plasma_client(db=0)
        rb = StrictRedis('localhost', decode_responses=False)
        oids = mmap(lambda key: ObjectID(rb[dic[key]][:20]), dic)
        pins = pclient.get_buffers(oids, timeout_ms=10)
        return pins



def get_stock_daily(symbol='HPG', to=None, num_gap=0, resolution='1D'):
    import pandas as pd
    import requests
    TIME_GAP = 34128060  # 395 days
    headers = {
        'Connection': 'keep-alive',
        'sec-ch-ua': '"Google Chrome";v="93", " Not;A Brand";v="99", "Chromium";v="93"',
        'sec-ch-ua-mobile': '?0',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
        'sec-ch-ua-platform': '"Linux"',
        'Accept': '*/*',
        'Origin': 'https://chart.vps.com.vn',
        'Sec-Fetch-Site': 'same-site',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        'Referer': 'https://chart.vps.com.vn/',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    timestamp = int(dt.strptime(dt.now().strftime('%Y_%m_%d'), '%Y_%m_%d').timestamp())
    params = (
        ('symbol', symbol),
        ('resolution', resolution),
        ('from', str(timestamp-TIME_GAP*(num_gap + 1))),
        ('to', str(timestamp-TIME_GAP*num_gap)),
    )

    response = requests.get(
        'https://histdatafeed.vps.com.vn/tradingview/history',
        headers=headers,
        params=params)
    df = pd.DataFrame(response.json())
    df['t'] = df['t'].map(lambda x: dt.fromtimestamp(x - 3600 * 7))
    df.rename(columns={
        'c': 'close',
        'o': 'open',
        'h': 'high',
        'l': 'low',
        'v': 'volume'}, inplace=True)
    df['day'] = df['t'].map(lambda x: x.strftime('%Y_%m_%d'))
    del df['s']
    df = df.reset_index(drop=True)
    df['stock'] = symbol
    return df


def get_vn30_daily(start='2021_03_00', end=None, symbol='VN30', to=None):
    import pandas as pd
    if end is None:
        from dc_server.redis_tree import REDIS_GLOBAL_TIME
        end = REDIS_GLOBAL_TIME.TODAY()
    import requests

    headers = {
        'Connection': 'keep-alive',
        'sec-ch-ua': '"Google Chrome";v="93", " Not;A Brand";v="99", "Chromium";v="93"',
        'sec-ch-ua-mobile': '?0',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
        'sec-ch-ua-platform': '"Linux"',
        'Accept': '*/*',
        'Origin': 'https://chart.vps.com.vn',
        'Sec-Fetch-Site': 'same-site',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        'Referer': 'https://chart.vps.com.vn/',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    if to is None: to = str(int(dt.now().timestamp()))
    params = (
        ('symbol', symbol),
        ('resolution', '1D'),
        ('from', '1598328077'),
        ('to', to),
    )

    response = requests.get(
        'https://histdatafeed.vps.com.vn/tradingview/history',
        headers=headers,
        params=params)
    df = pd.DataFrame(response.json())
    df['t'] = df['t'].map(lambda x: dt.fromtimestamp(x - 3600 * 7))
    df.rename(columns={
        'c': 'close',
        'o': 'open',
        'h': 'high',
        'l': 'low',
        'v': 'volume'}, inplace=True)
    df['day'] = df['t'].map(lambda x: x.strftime('%Y_%m_%d'))
    df = df[df['day'] >= start]
    df = df[df['day'] <= end]
    del df['s']
    df = df.reset_index(drop=True)
    return df


def top_based_list_pids(verbosity=1):
    import pandas as pd

    pd.set_option('display.width', 220)
    pd.set_option('display.max_columns', 200)
    pd.set_option('display.max_rows', 100)
    NUM_SHOWN = 30

    #cmd = "top -o +%MEM -b -n1"
    cmd = "top -b -n1"

    text = execute_cmd(cmd, print_result=False)
    lines = text.split('\n')
    for LABEL_ROW_INDEX in range(len(lines)):
        if 'PID' in lines[LABEL_ROW_INDEX]: break

    def is_time(s):
        return ('1' + s.replace(':', '').replace('.', '')).isnumeric() and ':' in s

    lst = []
    for i, line in enumerate(lines[LABEL_ROW_INDEX + 1:]):
        if len(line) == 0: continue
        try:
            l = [x for x in line.split(' ') if len(x.replace(' ', '')) != 0]
            assert sum(list(map(is_time, l))) == 1
            for index in range(len(l)):
                if is_time(l[index]) > 0:
                    break
            dic = {
                'pid': l[0],
                'usr': l[1],
                'time': l[index],
                'cmd': ' '.join(l[index + 1:]),
                'mem': l[index - 1],
                'cpu': l[index - 2],
                's': l[index - 3],
                'line': line
            }
            lst.append(dic)
        except Exception as e:
            print(f'{i} {line} \x1b[93m{e}\x1b[0m')

    df = pd.DataFrame(lst)
    df['mem'] = df['mem'].map(lambda x: float(x.replace(',', '.')))
    df['cpu'] = df['cpu'].map(lambda x: float(x.replace(',', '.')))

    df = df.sort_values('cpu', ascending=False).reset_index(drop=True)
    if verbosity >= 2:
        print(df.head(NUM_SHOWN))
        print(f"CPU: total=\x1b[93m{df['cpu'].sum():,.1f}%\x1b[0m top_{NUM_SHOWN}=\x1b[93m"
              f"{df.iloc[:NUM_SHOWN]['cpu'].sum():,.1f}%\x1b[0m\n")
        df = df.sort_values('mem', ascending=False).reset_index(drop=True)
        print(df.head(NUM_SHOWN))
        print(f"MEMORY: total=\x1b[93m{df['mem'].sum():,.1f}%\x1b[0m top_{NUM_SHOWN}=\x1b[93m"
              f"{df.iloc[:NUM_SHOWN]['mem'].sum():,.1f}%\x1b[0m")
    return df


def ps_aux_based_list_pids(sort_by='cpu', verbosity=1):
    import pandas as pd

    pd.set_option('display.width', 220)
    pd.set_option('display.max_columns', 200)
    pd.set_option('display.max_rows', 100)

    if get_host_name() != 'mac':
        lines = [x for x in execute_cmd('ps -aux', print_result=False).split('\n') if len(x) > 0]
    else:
        lines = [x for x in execute_cmd('ps aux', print_result=False).split('\n') if len(x) > 0]
    column_names = [x.lower().replace('%', '') for x in lines[0].split(' ')][:-1]
    lines = lines[1:]
    df = pd.DataFrame([x.split(' ') for x in lines]).fillna("")
    f = lambda x: '' if x.replace(':', '').replace('.', '').isnumeric() else x
    df['cmd'] = df[df.columns[9:]].apply(
        lambda x: ' '.join([z.replace('[', '').replace(']', '') for z in [f(y) for y in x] if len(z) > 0]), axis=1)
    df['count'] = df['cmd'].map(lambda x: len(x.replace(' ', '')))
    df['text'] = lines

    df.columns = column_names + df.columns.tolist()[len(column_names):]

    def maybe_float(x):
        try:
            return float(x)
        except Exception as e:
            if len(str(e)) == 0: print(e)
            return 0

    df['cpu'] = df['cpu'].map(maybe_float)
    df['mem'] = df['mem'].map(maybe_float)
    df = df.sort_values('cpu')

    if verbosity >= 2:
        for i in range(len(df) - 50, len(df)): print(f"\x1b[94mcpu=\x1b[93m{df.iloc[i]['cpu']:4} \x1b[96m",
                                                 df.iloc[i]['cmd'], "\x1b[0m", df.iloc[i]['text'])

        print()
        df = df.sort_values('mem')
        for i in range(len(df) - 15, len(df)): print(f"\x1b[94mmem=\x1b[93m{df.iloc[i]['mem']:4} \x1b[96m",
                                                     df.iloc[i]['cmd'], "\x1b[0m", df.iloc[i]['text'])
        print('')
    if verbosity >= 1:
        print(f'Total cpu usage = \x1b[92m{df["cpu"].sum():,.0f}%\x1b[0m')
        print(f'Total memmory usage = \x1b[92m{df["mem"].sum():,.0f}%\x1b[0m')

    df = df[column_names + ['count', 'cmd', 'text']]
    return df.sort_values(sort_by).reset_index(drop=True)


def list_pids(verbosity=1):
    def list_pids_helper(sort_by='cpu'):
        df_ps = ps_aux_based_list_pids(verbosity=0)
        df_ps = df_ps.rename(columns={'cpu': 'cpu2', 'mem': 'mem2', 'cmd': 'full_cmd'})
        df_ps = df_ps[['pid', 'vsz', 'rss', 'tty', 'full_cmd', 'cpu2', 'mem2']]
        df = top_based_list_pids(verbosity=0)
        del df['line']
        return df_ps, df, df.merge(df_ps, on='pid', how='left').sort_values(sort_by, ascending=False).reset_index(
            drop=True)

    def print_line(i, dic):
        print(f"#{str(i).ljust(4)} \x1b[32m{dic['pid'].rjust(9)}\x1b[0m "
              f"cpu=\x1b[93m{dic['cpu']}, {dic['cpu2']}\x1b[0m  "
              f"mem=\x1b[94m{dic['mem']}, {dic['mem2']}\x1b[0m".ljust(NUM_SHOWN, ' ') + f" \x1b[95m{dic['usr']}\x1b[0m "
                                                                                        f" \x1b[96m{dic['cmd']}\x1b[0m "
                                                                                        f"{dic['full_cmd']}")

    NUM_SHOWN = 76
    MIN_PCT = 0.1
    df_ps, df_top, df = list_pids_helper(sort_by='mem')
    for i in range(NUM_SHOWN):
        dic = df.iloc[i].to_dict()
        if dic['mem'] < MIN_PCT: break
        print_line(i, dic)

    print("\n")

    df = df.sort_values('cpu', ascending=False)
    for i in range(NUM_SHOWN):
        dic = df.iloc[i].to_dict()
        if dic['cpu'] < MIN_PCT: break
        print_line(i, dic)

    if verbosity >= 1:
        print(f"cpu=\x1b[92m{df['cpu'].sum():,.1f}%, {df['cpu2'].sum():,.1f}%\x1b[0m  "
              f"mem=\x1b[92m{df['mem'].sum():,.1f}%, {df['mem2'].sum():,.1f}%\x1b[0m")
    return df


def init_stuff():
    import pandas as pd
    pd.set_option('display.width', 270)
    pd.set_option('display.max_columns', 200)
    pd.set_option('display.max_rows', 100)

    G.DAY = dt.now().strftime('%Y_%m_%d')


class IDs:
    BANVIET = '068P999999'
    HSC = '011P000001'
    BDS_SSI = '003C110088'
    BLUE_POINT = '068C039049'
    KIS = '057ECB5693'
    BAO_VIET = '001P000000'
    SUU_VFM_VN30 = 'SCBB609999'
    MB = '005P000000'
    LTL = '091C916004'
    SSI = '003P888888' # CtyCP CK Sài Gòn
    JP_MORGAN = 'HSBFCS1965'
    NOYA = '003C168986'
    FETA = '003C131868'
    VEIL = 'SCBFC00003'
    TRUONG_MONEY = '077C080963'
    NGUYEN_KHANH_VINH = '077C700292'
    # https://dragoncapital.com.vn/r/vfmvsf-bao-cao-gia-tri-tai-san-rong-tuan-12-10-2021/
    HANOI_INVESTMENT_HOLDING = 'SCBFCB2317' # Quỹ Đầu tư Cổ phiếu Việt Nam chọn lọc (VFMVSF) - Dragon ...
    VFM_VSF = 'SCBB906666'
    FUBON = 'HSBFCC7530'
    ALL = {'077ECA9645', 'CTBFCB0361', 'HSBFCC7530', 'IVBB000001', 'HSBB081306', '006P000002', '021P222222',
           'HSBFCA6235', 'VCHB000102', 'SCBB008888', 'CTBFCS8902', '022P002222', '011P000001', 'HSBFCS2687',
           'BIDB558885', 'SCBB609999', '003P888888', 'SCBB669999', '057ECB5693', 'SCBB608888', '001P000000',
           '003C110088', 'HSBFCC0247', '068C039049', 'HSBFCS2386', 'VCHB668888', '005P000000', 'CTBFCA2146',
           '068P999999', 'HSBFCA9120', '003C168986', '003C131868', 'SCBFC00003', 'HSBFCC7530'}
    ARBITERS = []


IDs.ARBITERS = [IDs.KIS, IDs.SSI, IDs.HSC, IDs.BLUE_POINT, IDs.BDS_SSI, IDs.SSI, IDs.BANVIET, IDs.SUU_VFM_VN30]

class IDs2:
    MPT = '003C036316'
    NGUYEN_MINH_TUONG = '003C123888'
    GERMANY_PENM_IV = 'SCBFCA8956'
    TA_CONG_SON = '011C098765'
    DO_ANH_VIET = '026C018882'
    LE_THI_LIEN = '091C916004'
    NOYA = '003C168986'


class BUBBLE_PLASMA_KEYS:
    DF_GROUPED = 'plasma.bubble.df_grouped'
    DFS_BUBBLES = 'plasma.bubble.bubbles'
    DF_ARBIT4 = 'plasma.bubble.arbit4'


class Base_Adapter:
    @staticmethod
    def write_binary_to_mongodb(data, coll, do_drop=False, verbosity=2):
        import zlib
        from bson import Binary
        from bson.objectid import ObjectId
        # noinspection PyUnresolvedReferences
        import pickle
        data = zlib.compress(pickle.dumps(data))
        def chunks(lst, n):
            if len(lst) == 0: return []
            assert n > 0
            current = 0
            while current < len(lst):
                yield lst[current: min(current + n, len(lst))]
                current += n

        header = '\x1b[90mwrite_binary_to_mongodb(\x1b[0m: '
        print(header)
        client = coll.database.client
        if do_drop: coll.drop()
        if len(data) > 16_000_001:
            for chunk in chunks(data, 16_000_000):
                res = coll.insert_one({'pickledData': Binary(chunk)})
                assert type(res.inserted_id) == ObjectId, \
                    'Loi: write_binary_to_mongodb()'
                print(f'Inserted into \x1b[91m{client.address}'
                      f'["{coll.database.name}"]["{coll.name}"]'
                      f'\x1b[0m: {res.inserted_id}')
        else:
            res = coll.insert_one({'pickledData': Binary(data)})
            assert type(res.inserted_id) == ObjectId, \
                'Loi: write_binary_to_mongodb()'
            print(f'Inserted into \x1b[91m'
                  f'{client.address}["{coll.database.name}"]'
                  f'["{coll.name}"]\x1b[0m: '
                  f'{res.inserted_id}')
        if verbosity >= 2:
            print(f'Successfully wrote \x1b[93m{len(data) / 1048576:,.2f} MB\x1b[0m '
                  f'into db: {client.address}["{coll.database.name}"]["{coll.name}"]')

    @staticmethod
    def read_pickled_binary_from_mongodb(coll, verbosity=1):
        from functools import reduce
        import pickle
        import zlib
        header = '\x1b[90mread_pickled_binary_from_mongodb\x1b[0m: '
        print(header)
        binary_data = reduce(
            lambda a, b:
            a + b, map(lambda x: x['pickledData'],
                       coll.find({}, {'_id': 0})))
        unzipped = zlib.decompress(binary_data)
        dic = pickle.loads(unzipped)
        if verbosity >= 1:
            print(dic.keys())
            print(f'Successfully read \x1b[93m{len(unzipped) / 1048576:,.2f} MB\x1b[0m '
                  f'from db: \x1b[91m{coll.database.client.address}'
                  f'["{coll.database.name}"]["{coll.name}"\x1b[0m]')
        return dic


def load_df_weights():
    import pandas as pd
    from dc_server.redis_tree import REDIS_GLOBAL_TIME
    from redis import StrictRedis
    rlv2 = StrictRedis(host='lv2', decode_responses=True)
    WEIGHTS = json.loads(rlv2.get('weight_vn30'))
    dfw = pd.DataFrame(WEIGHTS)
    last_known_day = dfw.columns.values[-1]
    missing_days = [x for x in n_tradingdays_from_day(last_known_day, 100) if x <= REDIS_GLOBAL_TIME.LAST_TRADING_DAY()]
    for day in missing_days:
        dfw[day] = dfw[last_known_day]
    dfw = dfw / 100
    dfw = dfw.reset_index().rename(columns={'index': 'stock'})
    dfw = pd.melt(dfw, id_vars=['stock'], value_vars=[x for x in dfw.columns if '2021' in x],
                  value_name='weight', var_name='day')
    return dfw


def load_huge_from_plasma(skip_dics=False, restore=[]):
    import pandas as pd
    from dc_server.config import CONFIG

    DB_MASTER = CONFIG[get_host_name()]["DB_MASTER"]
    _, _, psave, pload = gen_plasma_functions(db=DB_MASTER)
    start = time()
    print(f"{Tc.CBEIGE2}    Loading df_huge (NUMERICAL) {Tc.CEND}")
    df_huge = pload(MASTER_PLASMA_KEYS.HUGE_NUMERICAL)
    if skip_dics: return df_huge
    ### Load df_dic from plasma and reconstruct mappings ###
    print(f"{Tc.CBEIGE2}    Loading df_dic from plasma and reconstruct mappings {Tc.CEND}")
    dic_df, dic_df_r = {}, {}
    tik()
    temp = pd.DataFrame(pload(MASTER_PLASMA_KEYS.HUGE_STR)) # Costs around 1200 mb ram
    temp[1] = temp[1].map(int)
    tok()
    for dic in mmap(lambda x: {x[0]: {k: v for k, v in zip(x[1][1], x[1][2])}}, temp.groupby(0)):
        col = list(dic.keys())[0]
        mapping = list(dic.values())[0]
        dic_df[col] = mapping
        dic_df_r[col] = {value: key for key, value in mapping.items()}
    for col in restore:
        df_huge[col] = df_huge[col].map(lambda x: dic_df[col][x])
    print(f'Time taken load from plasma -> rebuild dic: {time() - start:.2f}',)

    return df_huge, dic_df, dic_df_r


def chunks(lst, n):
    if len(lst) == 0: return []
    assert n > 0
    current = 0
    while current < len(lst):
        yield lst[current: min(current+n, len(lst))]
        current += n


def yield_rows(cursor, chunk_size):
    chunk = []
    for i, row in enumerate(cursor):
        if i % chunk_size == 0 and i > 0:
            yield chunk
            del chunk[:]
        chunk.append(row)
    yield chunk


def write_to_pickle(data, fn='/tmp/liquidity/df_tickdata.pickle'):
    maybe_create_dir('/'.join(fn.split('/')[:-1]))
    with open(fn, 'wb') as file: pickle.dump(data, file)
    execute_cmd(f'ls -lahtr {fn}')


def save_and_view(fn, save_file):
    from bokeh.util.browser import view
    maybe_create_dir('/'.join(fn.split('/')[:-1]))
    save_file(fn)
    view(fn)


def start_slave_sotola(host, port=22, ip=None):
    python_fn = '/home/ubuntu/anaconda3/lib/python3.8/dc_server/production/dc_slave_sotola.py'
    output = execute_cmd(f" ssh -p {port} ubuntu@{host} '/home/ubuntu/anaconda3/bin/python {python_fn}'")
    print(f"Done with \x1b[93m{host}\x1b[0m ({ip})")
    return output


def start_slave_master(host, port=22, ip=None):
    python_fn = '/home/ubuntu/anaconda3/lib/python3.8/dc_server/production/slavemaster_starter.py'
    output = execute_cmd(f" ssh -p {port} ubuntu@{host} '/home/ubuntu/anaconda3/bin/python {python_fn}'")
    print(f"Done with \x1b[93m{host}\x1b[0m ({ip})")
    return output


def prioritize_df(df, columns):
    cols = columns + [x for x in df.columns if x not in columns]
    return df[cols]


def start_slave_tom(host, port=22, ip=None):
    python_fn = '/home/ubuntu/anaconda3/lib/python3.8/dc_server/production/dc_slave_tom.py'
    output = execute_cmd(f" ssh -p {port} ubuntu@{host} '/home/ubuntu/anaconda3/bin/python {python_fn}'")
    print(f"Done with \x1b[93m{host}\x1b[0m ({ip})")
    return output


def start_slave_masters():
    from dc_server.lazy_core import execute_cmd

    configs = [
        {
            'host': 'ws',
            'ip': '113.161.34.115',  # 192.168.1.19
            'port': 22222
        },
        {
            'host': 'mx',
            'ip': '113.161.34.115', # 192.168.1.5
            'port': 1193
        },

        {
            'host': 'lv2',
            'ip': '45.119.214.155',
            'port': 22
        }
    ]
    hosts = map(lambda x: x['host'], configs)
    print(f"Running load_slave_masters for \x1b[93m{hosts}\x1b[0m")
    for config in configs:
        start_slave_master(**config)
    print(f"All Dones ({hosts})")

def plotly_view(fig, fn, show=True):
    from bokeh.util.browser import view
    dir_fn = '/'.join(fn.split('/')[:-1])
    maybe_create_dir('/tmp/vwap_plots/')
    fig.write_html(fn)
    if show: view(fn)

###################################################### NEW STUFF ######################################################
#%%

DAY0_NUMERICAL_REPR = 10_000_000
DAY0_STR_REPR = '0000_00_00'

def csv_ssi_ws_vn30(day):
    list_name = f'list_ssi_ws_vn30.{day}.csv'
    return list_name


def auto_update_day():
    from dc_server.redis_tree import REDIS_GLOBAL_TIME
    # noinspection PyGlobalUndefined
    global _IS_AUTO_UPDATING_DAY

    def stop_auto_update_day():
        # noinspection PyGlobalUndefined
        global _IS_AUTO_UPDATING_DAY
        _IS_AUTO_UPDATING_DAY = False
    def helper():
        # noinspection PyGlobalUndefined
        global _IS_AUTO_UPDATING_DAY
        while _IS_AUTO_UPDATING_DAY:
            G.DAY = REDIS_GLOBAL_TIME.LAST_TRADING_DAY()
            sleep(10)

    G.DAY = REDIS_GLOBAL_TIME.LAST_TRADING_DAY()
    if '_IS_AUTO_UPDATING_DAY' not in globals() or _IS_AUTO_UPDATING_DAY == False:
        _IS_AUTO_UPDATING_DAY = True
        threading_func_wrapper(helper)
        return stop_auto_update_day
    else:
        print(f'{Tc.CYELLOW2}call to auto_update_day(): auto_update_day is already turned on, current DAY is {G.DAY} {Tc.CEND}')
        return stop_auto_update_day


class G:
    DAY = '0000_00_00'


def clear_redis():
    from dc_server.redis_tree import REDIS_GLOBAL_TIME
    from redis import StrictRedis

    r = StrictRedis('localhost', decode_responses=True)
    TODAY = REDIS_GLOBAL_TIME.TODAY()
    for key in sorted([x for x in r.keys() if 'redis_lists.node.js' in x and TODAY not in x]):
        print(key)
        assert r.delete(key) == 1
    print('Done deleteing all keys')


def repin(key, db, verbosity=1):
    from pyarrow._plasma import ObjectID, ObjectNotAvailable
    from redis import StrictRedis

    rb = StrictRedis('localhost', decode_responses=False)
    binary_key = rb[key][:20]
    plasma_key = ObjectID(binary_key)
    pclient = get_plasma_client(db=db)
    obj = pclient.get(plasma_key, timeout_ms=10)

    if type(obj) == type:
        print(f"{dt.now().strftime('%H:%M:%S')}: {Tc.CBLUEBG2}{Tc.CBLACK}{key}{Tc.CEND}", "plasma key not found")
        print(ObjectNotAvailable)
        if verbosity >= 2: print(f'Plasma object not found for key={key}')
        return None, None

    oid = plasma_key
    buffer = pclient.get_buffers([oid])[0]
    if verbosity >= 2: print(f'found {Tc.CGREY}{key}{Tc.CEND} in plasma')
    return oid, buffer


def generate_class_IDs_INT(mw):
    s = """
    class IDs:
        BANVIET = '068P999999'
        HSC = '011P000001'
        BDS_SSI = '003C110088'
        BLUE_POINT = '068C039049'
        KIS = '057ECB5693'
        BAO_VIET = '001P000000'
        SUU_VFM_VN30 = 'SCBB609999'
        MB = '005P000000'
        SSI = '003P888888' # CtyCP CK Sài Gòn
        LTL = '091C916004'
        JP_MORGAN = 'HSBFCS1965'
        NOYA = '003C168986'
        FETA = '003C131868'
        # https://dragoncapital.com.vn/r/vfmvsf-bao-cao-gia-tri-tai-san-rong-tuan-12-10-2021/
        HANOI_INVESTMENT_HOLDING = 'SCBFCB2317' # Quỹ Đầu tư Cổ phiếu Việt Nam chọn lọc (VFMVSF) - Dragon ...
        VFM_VSF = 'SCBB906666'
        ALL = {'077ECA9645', 'CTBFCB0361', 'HSBFCC7530', 'IVBB000001', 'HSBB081306', '006P000002', '021P222222',
               'HSBFCA6235', 'VCHB000102', 'SCBB008888', 'CTBFCS8902', '022P002222', '011P000001', 'HSBFCS2687',
               'BIDB558885', 'SCBB609999', '003P888888', 'SCBB669999', '057ECB5693', 'SCBB608888', '001P000000',
               '003C110088', 'HSBFCC0247', '068C039049', 'HSBFCS2386', 'VCHB668888', '005P000000', 'CTBFCA2146',
               '068P999999', 'HSBFCA9120', '003C168986', '003C131868'}
        ARBITERS = []
    """

    s_int = ''

    i = 0
    while i + 1 < len(s):
        i += 1
        c = s[i]
        if c != "'":
            s_int += c
            continue
        the_id = ''
        j = i + 1
        while s[j] != "'":
            the_id += s[j]
            j += 1
        s_int += str(mw.s2i.dic_df_r['id'][the_id])
        i = j
    print(s_int)


class IDs_INT:
    BANVIET = 133
    HSC = 354
    BDS_SSI = 109256
    BLUE_POINT = 77267
    KIS = 84825
    BAO_VIET = 288
    SUU_VFM_VN30 = 102648
    MB = 85
    SSI = 63261 # CtyCP CK Sài Gòn
    LTL = 64644
    JP_MORGAN = 20
    NOYA = 469316
    FETA = 111205
    # https://dragoncapital.com.vn/r/vfmvsf-bao-cao-gia-tri-tai-san-rong-tuan-12-10-2021/
    HANOI_INVESTMENT_HOLDING = 32 # Quỹ Đầu tư Cổ phiếu Việt Nam chọn lọc (VFMVSF) - Dragon ...
    VFM_VSF = 68441
    ALL = {25525, 111180, 95832, 274336, 173, 289, 4987,
           111292, 271278, 111143, 52938, 2661, 354, 111707,
           222, 102648, 63261, 111360, 84825, 95968, 288,
           109256, 111355, 77267, 111482, 69011, 85, 3737,
           133, 111176, 469316, 111205}
    ARBITERS = []

IDs_INT.ARBITERS = [IDs_INT.KIS, IDs_INT.SSI, IDs_INT.HSC, IDs_INT.BLUE_POINT,
                    IDs_INT.BDS_SSI, IDs_INT.SSI, IDs_INT.BANVIET, IDs_INT.SUU_VFM_VN30]
DB_ENV_HOSE500 = 'env_hose500'
DB_ARBIT_V4 = 'arbit_v4'


def time_int_to_x(t):
    from dc_server.sotola import FRAME

    t = str(t)
    t = t[2:4] + ':' + t[4:6] + ':' + t[6:8]
    if '09:15:00' > t:
        t = '09:15:00'
    elif '11:30:59' < t < '13:00:00':
        t = '11:30:59'
    elif t > '14:45:59':
        t = '14:45:59'

    return FRAME.timeToIndex[t]


def print_plasma_log(db=0, n=40):
    from redis import StrictRedis
    r = StrictRedis(decode_responses=True)
    key = f'debug.plasma.{db}'
    lst = r.lrange(key, -n, -1)
    print(key, r.llen(key))
    return lst


def grab_ssi_ps():
    import requests, pandas as pd

    headers = {
        'authority': 'wgateway-iboard.ssi.com.vn',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="96", "Google Chrome";v="96"',
        'accept': '*/*',
        'sec-ch-ua-mobile': '?0',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
        'sec-ch-ua-platform': '"Linux"',
        'origin': 'https://iboard.ssi.com.vn',
        'sec-fetch-site': 'same-site',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'referer': 'https://iboard.ssi.com.vn/',
        'accept-language': 'en-US,en;q=0.9',
    }

    json_data = {
        'operationName': 'leTables',
        'variables': {
            'stockNo': 'fu:1149',
        },
        'query': 'query leTables($stockNo: String) {\n  leTables(stockNo: $stockNo) {\n    '
                 'stockNo\n    price\n    accumulatedVol\n    time\n    vol\n    ref\n    '
                 'side\n    priceChange\n    priceChangePercent\n    changeType\n    '
                 '__typename\n  }\n  stockRealtime(stockNo: $stockNo) {\n    '
                 'stockNo\n    ceiling\n    floor\n    refPrice\n    '
                 'stockSymbol\n    __typename\n  }\n}\n',
    }

    response = requests.post('https://wgateway-iboard.ssi.com.vn/graphql',
                             headers=headers, json=json_data)
    data = response.json()['data']

    df = pd.DataFrame(data['leTables'])
    dic = {'bu': 1, 'sd': -1, 'unknown': 0}
    df['side'] = df['side'].map(dic)
    del df['__typename']
    return df.iloc[::-1]


def get_t_ago(seconds_ago=0):
    from dc_server.sotola import FRAME
    t = dt.now().strftime("%H:%M:%S")
    if t <= '09:00:00': t = '09:00:00'
    elif t >= '14:45:20': t = '14:45:20'
    elif '11:30:50' <= t < '13:00:00': t = '11:30:50'
    x = FRAME.timeToIndex[t]
    return FRAME.indexToTime[max(0, x - seconds_ago)]


def check_plasma_memory_usage(DB=0):
    import pandas as pd

    client = get_plasma_client(db=DB)
    dic = client.list()
    init_stuff()
    df = pd.DataFrame(dic.values())
    used_mem = df[df['ref_count'] > 0]['data_size'].sum()
    return {'percentage': round(100*used_mem / client.store_capacity()), 'used': used_mem, 'capacity': client.store_capacity()}


def get_plasma_metadata(key, db=1):
    from pyarrow._plasma import ObjectID, ObjectNotAvailable
    from redis import StrictRedis
    _, _, psave1, pload1 = gen_plasma_functions(db=db)
    client = get_plasma_client(db=1)
    r = StrictRedis(decode_responses=False)
    try:
        binary_key = r[key][:20]
        plasma_key = ObjectID(binary_key)
        return {plasma_key: client.list()[plasma_key]}
    except Exception as e:
        print(e)


def clear_redis_logs(delete=False):
    from dc_server.redis_tree import REDIS_GLOBAL_TIME
    import zlib
    _, _, psave, pload = gen_plasma_functions(1)
    from redis import StrictRedis

    for db in [0, 1, 2]:
        r = StrictRedis(decode_responses=True)
        key = f'debug.plasma.{db}'
        print(f'found {Tc.CYELLOW2}{r.llen(key):,d}{Tc.CEND} key(s) in in {Tc.CBEIGE2}{key}{Tc.CEND}')
        if r.llen(key) == 0: continue
        client = get_plasma_client(db)
        lst = r.lrange(key, 0, -1)
        for line in lst:
            if 'time=' in line: break
        day = line.split('time=')[1].split(' ')[0]
        maybe_create_dir('/d/tmp')
        fn = f'/d/tmp/db{db}_log_{day}_to_{REDIS_GLOBAL_TIME.TODAY()}.pickle.zip'
        data = zlib.compress(pickle.dumps(lst))
        print(f'{len(data) / 2 ** 20:,.0f} MB')
        with open(fn, 'wb') as file:
            file.write(data)
        execute_cmd(f'ls {fn} -lahtr')
        if delete: r.delete(key)


def get_arbit3(r=None, day=None):
    import pandas as pd
    if r is None:
        from redis import StrictRedis
        r = StrictRedis('lv2')
    if day is None:
        from dc_server.redis_tree import REDIS_GLOBAL_TIME
        day = REDIS_GLOBAL_TIME.TODAY()
    df = pd.DataFrame(json.loads(r.get(f'trala.lv2.input.arbit_v3.arbit_v3_summary.{day}')))
    arbit_value = round(sum((df['type'] == 'arbit') * df['num_lot']) * 2.5, 1)
    unwind_value = round(sum((df['type'] == 'unwind') * df['num_lot']) * 2.5, 1)
    return arbit_value, unwind_value


def walk_through_files(TARGET, ext="*"):
    import os
    lst = []
    for root, dirs, files in os.walk(TARGET):
        for file in files:
            if ext == "*" or file.endswith(ext):
                lst.append(os.path.join(root, file))

    return lst


def get_coin_scraper_parameters_from_redis():
    def load_dic_report_local():
        from redis import StrictRedis
        r = StrictRedis(decode_responses=True)
        assert len(r.keys()) > 0, f'\x1b[32mCo loi xay ra: Redis co van de\x1b[0m'
        return json.loads(r['multi.config.coin.scrapers.reports'])

    def load_symbol_list_local():
        from redis import StrictRedis
        r = StrictRedis(decode_responses=True)
        assert len(r.keys()) > 0, f'\x1b[32mCo loi xay ra: Redis co van de\x1b[0m'
        return json.loads(r['multi.config.coin.srapers.symbol_list'])

    from redis import StrictRedis
    DIC_REPORTS = load_dic_report_local()
    HOST_NAME = StrictRedis(decode_responses=True)['sotola.hostname']
    ALL_SYMBOL_LIST = load_symbol_list_local()
    return DIC_REPORTS, HOST_NAME, ALL_SYMBOL_LIST


import pickle, zlib


def get_target_client_27021():
    from dc_server.lazy_core import secured_mongo_connector
    client = secured_mongo_connector('192.168.1.19', 27021)
    return client


def get_target_db_27021():
    return get_target_client_27021('coin_numerical')


def get_source_db_27021():
    return get_target_client_27021('coin')


def map_coll_to_lst(coll, verbosity=1):
    n = coll.count_documents({})
    i = 0
    cur = coll.find({})
    lst = []
    size = 0
    while True:
        try:
            dp = cur.next()
            i += 1
            lst.append(dp)
            size += len(dp['pickledData']) / 2 ** 20
            if verbosity >= 1:
                print(f"\rLoaded \x1b[92m{size:,.2f}\x1b[0m MB (#{i}/{n}) from "
                      f"{coll.database.client.address}['{coll.name}']", end=' ')

        except Exception as e:
            if verbosity >= 1: print("...Reached EOF.")
            break
    return lst


def read_binary_from_mongodb(coll, verbosity=1):
    from functools import reduce
    lst = map_coll_to_lst(coll)

    metadata = lst[0]['metadata'] if 'metadata' in lst[0] else None
    zipped_binary_data = reduce(lambda a, b: a + b, map(lambda x: x['pickledData'], lst))

    print(f"Done loading \x1b[92m"
          f"{len(zipped_binary_data) / 2 ** 20:,.2f} MB\x1b[0m ({coll.name})"
          f" from mongodb. Unzipping...", end="")
    data = pickle.loads(zlib.decompress(zipped_binary_data))
    print("Done! \x1b[92m[√]\x1b[0m")
    if metadata: data['metadata'] = metadata
    return data


def write_binary_to_mongodb(data, coll, metadata=None, do_drop=True):
    from bson import Binary
    from bson.objectid import ObjectId
    if metadata is None: metadata = {}
    if do_drop: coll.drop()
    if len(data) > 16_000_001:
        for chunk in chunks(data, 16_000_000):
            if not metadata is None:
                meta_ata = None
            res = coll.insert_one({
                'metadata': metadata,
                'pickledData': Binary(chunk)})

            assert type(res.inserted_id) == ObjectId, \
                'Loi: write_binary_to_mongodb()'
            print(f'Inserted into {Tc.CRED2}{coll.database.client.address}'
                  f'["{coll.database.name}"]["{coll.name}"]'
                  f'{Tc.CEND}: {res.inserted_id}')
    else:
        res = coll.insert_one({
            'metadata': metadata,
            'pickledData': Binary(data)})
        assert type(res.inserted_id) == ObjectId, \
            'Loi: write_binary_to_mongodb()'
        print(f'Inserted into {Tc.CRED2}{coll.database.client.address}'
              f'["{coll.database.name}"]["{coll.name}"]'
              f'{Tc.CEND}: {res.inserted_id}')


def grab_ssi_stock_list(to_redis=False):
    import requests
    import pandas as pd
    headers = {
        'authority': 'wgateway-iboard.ssi.com.vn',
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        # Already added when you pass json=
        # 'content-type': 'application/json',
        'origin': 'https://iboard.ssi.com.vn',
        'referer': 'https://iboard.ssi.com.vn/',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="100", "Google Chrome";v="100"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
    }

    json_data = {
        'operationName': 'stockRealtimes',
        'variables': {
            'exchange': 'hose',
        },
        'query': 'query stockRealtimes($exchange: String) {\n  stockRealtimes(exchange: $exchange) {\n    stockNo\n    ceiling\n    floor\n    refPrice\n    stockSymbol\n    stockType\n    exchange\n    matchedPrice\n    matchedVolume\n    priceChange\n    priceChangePercent\n    highest\n    avgPrice\n    lowest\n    nmTotalTradedQty\n    best1Bid\n    best1BidVol\n    best2Bid\n    best2BidVol\n    best3Bid\n    best3BidVol\n    best4Bid\n    best4BidVol\n    best5Bid\n    best5BidVol\n    best6Bid\n    best6BidVol\n    best7Bid\n    best7BidVol\n    best8Bid\n    best8BidVol\n    best9Bid\n    best9BidVol\n    best10Bid\n    best10BidVol\n    best1Offer\n    best1OfferVol\n    best2Offer\n    best2OfferVol\n    best3Offer\n    best3OfferVol\n    best4Offer\n    best4OfferVol\n    best5Offer\n    best5OfferVol\n    best6Offer\n    best6OfferVol\n    best7Offer\n    best7OfferVol\n    best8Offer\n    best8OfferVol\n    best9Offer\n    best9OfferVol\n    best10Offer\n    best10OfferVol\n    buyForeignQtty\n    buyForeignValue\n    sellForeignQtty\n    sellForeignValue\n    caStatus\n    tradingStatus\n    remainForeignQtty\n    currentBidQty\n    currentOfferQty\n    session\n    __typename\n  }\n}\n',
    }

    response = requests.post('https://wgateway-iboard.ssi.com.vn/graphql', headers=headers, json=json_data)
    data = response.json()
    df = pd.DataFrame(data['data']['stockRealtimes'])
    if to_redis:
        from redis import StrictRedis
        r = StrictRedis(decode_responses=True)
        r['fallback.ssi_snapshot'] = json.dumps(transform_df_to_dic(df))
    return df


def load_ssi_ws_hose500_from_plasma(day=None):
    import pandas as pd
    from datetime import datetime as dt
    # from dc_server.lazy_core import gen_plasma_functions, init_stuff
    from dc_server.ssi_ws.ssi_ws_stock_parser import BASE_PARSER
    if day is None: day = dt.now().strftime('%Y_%m_%d')

    init_stuff()
    _, _, _, pload = gen_plasma_functions(db=0)

    df = pd.DataFrame(pload(f'plasma.node.js.hose500.numpy.{day}'))
    df.columns = BASE_PARSER.NUMERICAL_COLUMNS

    dic_id_to_stock = BASE_PARSER.get_ssi_id_to_stock(hardcoded=True)
    df['code'] = df['id'].map(dic_id_to_stock)

    return df


def is_ipython_instance():
    import sys
    return 'pydevconsole.py' in sys.argv[0] or 'ipython' in sys.argv[0]


class Soto_Reports:
    @staticmethod
    def coin_future_scraper_depth_redis_address(symbol):
        return f'coin.future.scrapers.{symbol}.depth.report'

    @staticmethod
    def coin_future_scraper_redis_address(symbol):
        return f'coin.future.scrapers.{symbol}.alltrades.report'

    @staticmethod
    def coin_future_scraper_agg_trade_redis_address(symbol):
        return f'coin.future.scrapers.{symbol}.add_trade.report'


def list_processes(sort_by='cpu', trunc=False, pattern='python'):
    import psutil
    import pandas as pd

    def shorten(x):
        if trunc:
            return x[0]\
                .replace('/home/ubuntu/anaconda3/lib/python3.8/dc_server', 'DC_SERVER/')\
                .replace('/media/ubuntu', '')\
                .replace('/home/ubuntu/hung/Production', 'HUNG')\
                .replace('/home/ubuntu/tom', 'TOM') if len(x) >= 1 else ''
        else:
            return x[0] if len(x) >= 1 else ''

    def get_cwd(p):
        try:
            return p.cwd()
        except Exception as e:
            if len(str(e)): print(e)
            return 'error: ' + str(e)

    processes = [proc for proc in psutil.process_iter()]

    lst = []
    for p in processes:
        try:
            cmd = p.cmdline()
            if pattern != '*' and len(cmd) > 0 and f'{pattern} ' == cmd[0][:5]:
                cmd = cmd[0].split(' ')
            if pattern == '*' or pattern in cmd:
                lst.append({
                    'pid': p.pid,
                    'start_time': dt.fromtimestamp(p.create_time()).strftime('%H:%M:%S'),
                    'command': shorten([x for x in cmd if '.py' in x]),
                    'full_command': cmd,
                    'cpu': p.cpu_percent(),
                    'cwd': get_cwd(p),
                    'mem': f"{round(128*1024*p.memory_percent(), 2):,.0f}",
                })
        except Exception as e:
            print(e)

    return pd.DataFrame(lst).sort_values(sort_by, ascending=False).reset_index(drop=True)


def dbug(text=''):
    """
    example: text = 'def process_inbound_suggestion(json_string, url, ip, MAX_LENGTH=20, glob=glob_obj)'
    :return:
    """
    if len(text) == 0: text = 'def process_inbound_suggestion(json_string, url, ip, MAX_LENGTH=20, glob=glob_obj)'
    if 'def ' not in text: text += 'def '
    line = [x for x in text.replace('\n', ' ').split('def ') if len(x) > 0][0]
    variable_names = [x.split('=')[0] for x in line.split('(')[1].split(')')[0].split(', ')]
    variable_names = [x for x in variable_names if x not in {'glob', 'glob_obj'}]
    function_name = line.split('(')[0]

    braces = "{}"
    left, right = '{', "}"
    print(f"if DEBUG_{function_name.upper()}:")
    print(f'    global glob_{function_name}')
    print(f'    glob_{function_name} = {braces}')
    var = variable_names[0]
    for var in variable_names:
        print(f'    glob_{function_name}["{var}"] = {var}')
    #####################################
    print(f"\n\nDEBUG_{function_name.upper()} = True\n")
    print(f"if DEBUG_{function_name.upper()}:")
    for var in variable_names:
        print(f'    {var} = glob_{function_name}["{var}"]')


class PLASMA_KEYS:
    ARBIT4_CLUSTER = 'plasma.arbit4.cluster'


def load_dang_weights_from_db(day=None):
    from pymongo import MongoClient
    import pandas as pd
    db = MongoClient('ws', 27021)['weights']
    coll = db['vn30_weights']
    query = {} if day is None else {'day':day}
    df_all = pd.DataFrame(list(coll.find({},{'_id':0})))
    df = df_all[df_all['day']==day].copy()
    if len(df) > 0: return df
    return df_all[df_all['day']==df_all['day'].max()].copy()


def get_df_tickdata_vn30(day=None, restore=True):
    from dc_server.ssi_ws.ssi_ws_base_parser import BASE_PARSER
    #####
    if day is None: day = dt.now().strftime('%Y_%m_%d')
    PADAPTERS.maybe_initiate()
    # noinspection PyArgumentList
    df = PADAPTERS.load_ssi_ws_hose500()
    df_weights = load_dang_weights_from_db(day=day)
    lst_stocks = df_weights['stock'].to_list()
    dic_id_to_stock = {k: v for k, v in BASE_PARSER.get_ssi_id_to_stock(hardcoded=True).items() if v in lst_stocks}
    dic_stock_to_id = {v: k for k, v in dic_id_to_stock.items()}
    df = df[df['id'].isin(dic_id_to_stock)].reset_index(drop=True)
    if restore:
        df['stock'] = df['id'].map(dic_id_to_stock)
    return df, df_weights


def compute_df_next_days(num_gaps=2):
    import pandas as pd
    df_days = pd.concat(map
        (
            lambda x: get_stock_daily('SSI', num_gap=x),
            range(num_gaps)
        ), ignore_index=True)\
        .groupby('day')\
        .last()\
        .reset_index()[['day']]

    def compute_next_trading_day(df_days):
        def calculate_the_next_week_day(day_now):
            if day_now.isoweekday() == 5:
                day_now += datetime.timedelta(days=3)
            elif day_now.isoweekday() == 6:
                day_now += datetime.timedelta(days=2)
            else:
                day_now += datetime.timedelta(days=1)
            return day_now
        import datetime
        today = datetime.datetime.strptime(df_days['day'].iloc[-1], '%Y_%m_%d')
        the_day = calculate_the_next_week_day(today)
        return the_day.strftime('%Y_%m_%d')

    df_days['prev'] = df_days['day'].shift(1)
    next_day = compute_next_trading_day(df_days)
    df_days['next'] = df_days['day'].shift(-1, fill_value=next_day)
    df_days = df_days.iloc[1:].reset_index(drop=True)
    return df_days.set_index('day')


threading_func_wrapper, get_threads = create_threading_func_wrapper(MAX_NUM_THREADS=100)
SSI_WS_PLASMA_DB = 0


def set_pd_format(n=2):
    import pandas as pd
    # noinspection PyTypeChecker
    if   n == 0:
        pd.options.display.float_format = '{:,.0f}'.format
    elif n == 1:
        pd.options.display.float_format = '{:,.1f}'.format
    elif n == 2:
        pd.options.display.float_format = '{:,.2f}'.format
    elif n == 3:
        pd.options.display.float_format = '{:,.3f}'.format
    elif n == 4:
        pd.options.display.float_format = '{:,.4f}'.format
    elif n == 5:
        pd.options.display.float_format = '{:,.5f}'.format
    elif n == 6:
        pd.options.display.float_format = '{:,.6f}'.format
    elif n == 7:
        pd.options.display.float_format = '{:,.7f}'.format
    elif n == 8:
        pd.options.display.float_format = '{:,.8f}'.format
    elif n == 9:
        pd.options.display.float_format = '{:,.9f}'.format
    elif n == 10:
        pd.options.display.float_format = '{:,.10f}'.format


class Module:
    def __init__(self):
        self.start_time = time()
        self.tik_text = ''
        self.verbosity = 1

module = Module()


def tik(text=''):
    module.start_time = time()
    module.tik_text = text


def tok():
    if module.verbosity < 1: return
    print("\x1b[90mtok()\x1b[0m: "
          "\x1b[96m{}\x1b[0m "
          "\x1b[93m{:,.1f}\x1b[0m ms seconds elapsed".format(
        module.tik_text,
        1000 * (time() - module.start_time)))


def maybe_create_dir_path(fn):
    path = '/'.join(fn.split('/')[:-1] + [''])
    maybe_create_dir(path)
    execute_cmd(f'ls -lahtr {path}')


def create_header(header):
    return f'\x1b[90m{header}\x1b[0m: '


def prioritize_columns(self, columns):
    if type(columns) == str: columns = [columns]
    columns2 = columns + [x for x in self.columns if x not in columns]
    return self[columns2]

###
if __name__ == "__main__":
    init_stuff()
    processes = list_processes(pattern='node')
    print(processes)


def detect_dup_columns_in_df(df, auto_remove=True):
    columns = list(df.columns)
    print(columns)
    dic = {}
    new_columns = []
    dup_columns = []
    for col in columns:
        if col not in dic:
            dic[col] = 0
            new_columns.append(col)
        else:
            i = 2
            coli = f'{col}{i}'
            while coli in dic:
                i += 1
                coli = f'{col}{i}'
            new_columns.append(coli)
            dup_columns.append(coli)
            dic[coli] = 0
    assert len(columns) == len(new_columns)
    if auto_remove:
        df.columns = new_columns
        for col in dup_columns:
            del df[col]
        return df
    return new_columns, dup_columns


def day_str_to_int(s):
    return int(s.replace('_', ''))


def day_int_to_str(n):
    s = str(n)
    return f'{s[:4]}_{s[4:6]}_{s[6:8]}'


def wait(minutes):
    count = minutes * 60
    while True:
        header = create_header('wait')
        print(f'\r{header}', f"{count}   ", end="")
        count -= 1
        if count <= 0: break
        else: sleep(1)


class GD_DAYS:
    holidays_dic = {
        '2023_01_02': 'nghi bu tet duong',
        '2023_01_20': 'tet am lich',
        '2023_01_23': 'tet am lich',
        '2023_01_24': 'tet am lich',
        '2023_01_25': 'tet am lich',
        '2023_01_26': 'tet am lich',
        '2023_04_29': 'gio to Hung vuong',
        '2023_04_30': '30 thang 4',
        '2023_05_01': 'Quoc te lao dong',
        '2023_05_02': 'nghi bu',
        '2023_05_03': 'nghi bu',
        '2023_09_02': 'Le Quoc Khanh',
        '2023_09_04': 'nghi bu quoc khanh',
        '2024_01_01': 'tet duong lich'
    }

    @staticmethod
    def get_trading_days_2023():
        from datetime import date, timedelta
        year = 2023
        num_weekdays = 0
        day_lst = []
        d = date(year, 1, 1)
        while d.year == year:

            if d.weekday() < 5:
                num_weekdays += 1
                # noinspection PyArgumentList
                day_str = d.strftime(format='%Y_%m_%d')
                if day_str not in GD_DAYS.holidays_dic:
                    day_lst.append(day_str)

            d += timedelta(days=1)

        return day_lst

    @staticmethod
    def get_maturity_dates(year):
        from datetime import date, timedelta
        # year = 2023
        dic_third_thursdays = {}
        # Iterate over the months of the year
        for month in range(1, 13):
            # Calculate the first day of the month
            d = date(year, month, 1)

            # Use the `weekday()` method to determine the day of the week (0 = Monday, 6 = Sunday)
            if d.weekday() == 3:
                # If the first day of the month is a Thursday, the third Thursday is in the same week
                third_thursday = d + timedelta(days=14)
            else:
                # If the first day of the month is not a Thursday, calculate the next Thursday
                first_thursday = d + timedelta(days=(3 - d.weekday()) % 7)
                third_thursday = first_thursday + timedelta(days=14)

            # noinspection PyArgumentList
            dic_third_thursdays[month] = third_thursday.strftime(format='%Y_%m_%d')
        return dic_third_thursdays

    @staticmethod
    def get_all_gd_days(min_day='2021_01_01', max_day=None):
        ALL_GD_DAYS = ['2019_11_14', '2019_11_15', '2019_11_18', '2019_11_19',
                       '2019_11_20', '2019_11_21', '2019_11_22', '2019_11_25',
                       '2019_11_26', '2019_11_27', '2019_11_28', '2019_11_29',
                       '2019_12_02', '2019_12_03', '2019_12_04', '2019_12_05',
                       '2019_12_06', '2019_12_09', '2019_12_10', '2019_12_11',
                       '2019_12_12', '2019_12_13', '2019_12_16', '2019_12_17',
                       '2019_12_18', '2019_12_19', '2019_12_20', '2019_12_23',
                       '2019_12_24', '2019_12_25', '2019_12_26', '2019_12_27',
                       '2019_12_30', '2019_12_31', '2020_01_02', '2020_01_03',
                       '2020_01_06', '2020_01_07', '2020_01_08', '2020_01_09',
                       '2020_01_10', '2020_01_13', '2020_01_14', '2020_01_15',
                       '2020_01_16', '2020_01_17', '2020_01_20', '2020_01_21',
                       '2020_01_22', '2020_01_30', '2020_01_31', '2020_02_03',
                       '2020_02_04', '2020_02_05', '2020_02_06', '2020_02_07',
                       '2020_02_10', '2020_02_11', '2020_02_12', '2020_02_13',
                       '2020_02_14', '2020_02_17', '2020_02_18', '2020_02_19',
                       '2020_02_20', '2020_02_21', '2020_02_24', '2020_02_25',
                       '2020_02_26', '2020_02_27', '2020_02_28', '2020_03_02',
                       '2020_03_03', '2020_03_04', '2020_03_05', '2020_03_06',
                       '2020_03_09', '2020_03_10', '2020_03_11', '2020_03_12',
                       '2020_03_13', '2020_03_16', '2020_03_17', '2020_03_18',
                       '2020_03_19', '2020_03_20', '2020_03_23', '2020_03_24',
                       '2020_03_25', '2020_03_26', '2020_03_27', '2020_03_30',
                       '2020_03_31', '2020_04_01', '2020_04_03', '2020_04_06',
                       '2020_04_07', '2020_04_08', '2020_04_09', '2020_04_10',
                       '2020_04_13', '2020_04_14', '2020_04_15', '2020_04_16',
                       '2020_04_17', '2020_04_20', '2020_04_21', '2020_04_22',
                       '2020_04_23', '2020_04_24', '2020_04_27', '2020_04_28',
                       '2020_04_29', '2020_05_04', '2020_05_05', '2020_05_06',
                       '2020_05_07', '2020_05_08', '2020_05_11', '2020_05_12',
                       '2020_05_13', '2020_05_14', '2020_05_15', '2020_05_18',
                       '2020_05_19', '2020_05_20', '2020_05_21', '2020_05_22',
                       '2020_05_25', '2020_05_26', '2020_05_27', '2020_05_28',
                       '2020_05_29', '2020_06_01', '2020_06_02', '2020_06_03',
                       '2020_06_04', '2020_06_05', '2020_06_08', '2020_06_09',
                       '2020_06_10', '2020_06_11', '2020_06_12', '2020_06_15',
                       '2020_06_16', '2020_06_17', '2020_06_18', '2020_06_19',
                       '2020_06_22', '2020_06_23', '2020_06_24', '2020_06_25',
                       '2020_06_26', '2020_06_29', '2020_06_30', '2020_07_01',
                       '2020_07_02', '2020_07_03', '2020_07_06', '2020_07_07',
                       '2020_07_08', '2020_07_09', '2020_07_10', '2020_07_13',
                       '2020_07_14', '2020_07_15', '2020_07_16', '2020_07_17',
                       '2020_07_20', '2020_07_21', '2020_07_22', '2020_07_23',
                       '2020_07_24', '2020_07_27', '2020_07_28', '2020_07_29',
                       '2020_07_30', '2020_07_31', '2020_08_03', '2020_08_04',
                       '2020_08_05', '2020_08_06', '2020_08_07', '2020_08_10',
                       '2020_08_11', '2020_08_12', '2020_08_13', '2020_08_14',
                       '2020_08_17', '2020_08_18', '2020_08_19', '2020_08_20',
                       '2020_08_21', '2020_08_24', '2020_08_25', '2020_08_26',
                       '2020_08_27', '2020_08_28', '2020_08_31', '2020_09_01',
                       '2020_09_03', '2020_09_04', '2020_09_07', '2020_09_08',
                       '2020_09_09', '2020_09_10', '2020_09_11', '2020_09_14',
                       '2020_09_15', '2020_09_16', '2020_09_17', '2020_09_18',
                       '2020_09_21', '2020_09_22', '2020_09_23', '2020_09_24',
                       '2020_09_25', '2020_09_28', '2020_09_29', '2020_09_30',
                       '2020_10_01', '2020_10_02', '2020_10_05', '2020_10_06',
                       '2020_10_07', '2020_10_08', '2020_10_09', '2020_10_12',
                       '2020_10_13', '2020_10_14', '2020_10_15', '2020_10_16',
                       '2020_10_19', '2020_10_20', '2020_10_21', '2020_10_22',
                       '2020_10_23', '2020_10_26', '2020_10_27', '2020_10_28',
                       '2020_10_29', '2020_10_30', '2020_11_02', '2020_11_03',
                       '2020_11_04', '2020_11_05', '2020_11_06', '2020_11_09',
                       '2020_11_10', '2020_11_11', '2020_11_12', '2020_11_13',
                       '2020_11_16', '2020_11_17', '2020_11_18', '2020_11_19',
                       '2020_11_20', '2020_11_23', '2020_11_24', '2020_11_25',
                       '2020_11_26', '2020_11_27', '2020_11_30', '2020_12_01',
                       '2020_12_02', '2020_12_03', '2020_12_04', '2020_12_07',
                       '2020_12_08', '2020_12_09', '2020_12_10', '2020_12_11',
                       '2020_12_14', '2020_12_15', '2020_12_16', '2020_12_17',
                       '2020_12_18', '2020_12_21', '2020_12_22', '2020_12_23',
                       '2020_12_24', '2020_12_25', '2020_12_28', '2020_12_29',
                       '2020_12_30', '2020_12_31', '2021_01_04', '2021_01_05',
                       '2021_01_06', '2021_01_07', '2021_01_08', '2021_01_11',
                       '2021_01_12', '2021_01_13', '2021_01_14', '2021_01_15',
                       '2021_01_18', '2021_01_19', '2021_01_20', '2021_01_21',
                       '2021_01_22', '2021_01_25', '2021_01_26', '2021_01_27',
                       '2021_01_28', '2021_01_29', '2021_02_01', '2021_02_02',
                       '2021_02_03', '2021_02_04', '2021_02_05', '2021_02_08',
                       '2021_02_09', '2021_02_17', '2021_02_18', '2021_02_19',
                       '2021_02_22', '2021_02_23', '2021_02_24', '2021_02_25',
                       '2021_02_26', '2021_03_01', '2021_03_02', '2021_03_03',
                       '2021_03_04', '2021_03_05', '2021_03_08', '2021_03_09',
                       '2021_03_10', '2021_03_11', '2021_03_12', '2021_03_15',
                       '2021_03_16', '2021_03_17', '2021_03_18', '2021_03_19',
                       '2021_03_22', '2021_03_23', '2021_03_24', '2021_03_25',
                       '2021_03_26', '2021_03_29', '2021_03_30', '2021_03_31',
                       '2021_04_01', '2021_04_02', '2021_04_05', '2021_04_06',
                       '2021_04_07', '2021_04_08', '2021_04_09', '2021_04_12',
                       '2021_04_13', '2021_04_14', '2021_04_15', '2021_04_16',
                       '2021_04_19', '2021_04_20', '2021_04_22', '2021_04_23',
                       '2021_04_26', '2021_04_27', '2021_04_28', '2021_04_29',
                       '2021_05_04', '2021_05_05', '2021_05_06', '2021_05_07',
                       '2021_05_10', '2021_05_11', '2021_05_12', '2021_05_13',
                       '2021_05_14', '2021_05_17', '2021_05_18', '2021_05_19',
                       '2021_05_20', '2021_05_21', '2021_05_24', '2021_05_25',
                       '2021_05_26', '2021_05_27', '2021_05_28', '2021_05_31',
                       '2021_06_01', '2021_06_02', '2021_06_03', '2021_06_04',
                       '2021_06_07', '2021_06_08', '2021_06_09', '2021_06_10',
                       '2021_06_11', '2021_06_14', '2021_06_15', '2021_06_16',
                       '2021_06_17', '2021_06_18', '2021_06_21', '2021_06_22',
                       '2021_06_23', '2021_06_24', '2021_06_25', '2021_06_28',
                       '2021_06_29', '2021_06_30', '2021_07_01', '2021_07_02',
                       '2021_07_05', '2021_07_06', '2021_07_07', '2021_07_08',
                       '2021_07_09', '2021_07_12', '2021_07_13', '2021_07_14',
                       '2021_07_15', '2021_07_16', '2021_07_19', '2021_07_20',
                       '2021_07_21', '2021_07_22', '2021_07_23', '2021_07_26',
                       '2021_07_27', '2021_07_28', '2021_07_29', '2021_07_30',
                       '2021_08_02', '2021_08_03', '2021_08_04', '2021_08_05',
                       '2021_08_06', '2021_08_09', '2021_08_10', '2021_08_11',
                       '2021_08_12', '2021_08_13', '2021_08_16', '2021_08_17',
                       '2021_08_18', '2021_08_19', '2021_08_20', '2021_08_23',
                       '2021_08_24', '2021_08_25', '2021_08_26', '2021_08_27',
                       '2021_08_30', '2021_08_31', '2021_09_01', '2021_09_06',
                       '2021_09_07', '2021_09_08', '2021_09_09', '2021_09_10',
                       '2021_09_13', '2021_09_14', '2021_09_15', '2021_09_16',
                       '2021_09_17', '2021_09_20', '2021_09_21', '2021_09_22',
                       '2021_09_23', '2021_09_24', '2021_09_27', '2021_09_28',
                       '2021_09_29', '2021_09_30', '2021_10_01', '2021_10_04',
                       '2021_10_05', '2021_10_06', '2021_10_07', '2021_10_08',
                       '2021_10_11', '2021_10_12', '2021_10_13', '2021_10_14',
                       '2021_10_15', '2021_10_18', '2021_10_19', '2021_10_20',
                       '2021_10_21', '2021_10_22', '2021_10_25', '2021_10_26',
                       '2021_10_27', '2021_10_28', '2021_10_29', '2021_11_01',
                       '2021_11_02', '2021_11_03', '2021_11_04', '2021_11_05',
                       '2021_11_08', '2021_11_09', '2021_11_10', '2021_11_11',
                       '2021_11_12', '2021_11_15', '2021_11_16', '2021_11_17',
                       '2021_11_18', '2021_11_19', '2021_11_22', '2021_11_23',
                       '2021_11_24', '2021_11_25', '2021_11_26', '2021_11_29',
                       '2021_11_30', '2021_12_01', '2021_12_02', '2021_12_03',
                       '2021_12_06', '2021_12_07', '2021_12_08', '2021_12_09',
                       '2021_12_10', '2021_12_13', '2021_12_14', '2021_12_15',
                       '2021_12_16', '2021_12_17', '2021_12_20', '2021_12_21',
                       '2021_12_22', '2021_12_23', '2021_12_24', '2021_12_27',
                       '2021_12_28', '2021_12_29', '2021_12_30', '2021_12_31',
                       '2022_01_04', '2022_01_05', '2022_01_06', '2022_01_07',
                       '2022_01_10', '2022_01_11', '2022_01_12', '2022_01_13',
                       '2022_01_14', '2022_01_17', '2022_01_18', '2022_01_19',
                       '2022_01_20', '2022_01_21', '2022_01_24', '2022_01_25',
                       '2022_01_26', '2022_01_27', '2022_01_28', '2022_02_07',
                       '2022_02_08', '2022_02_09', '2022_02_10', '2022_02_11',
                       '2022_02_14', '2022_02_15', '2022_02_16', '2022_02_17',
                       '2022_02_18', '2022_02_21', '2022_02_22', '2022_02_23',
                       '2022_02_24', '2022_02_25', '2022_02_28', '2022_03_01',
                       '2022_03_02', '2022_03_03', '2022_03_04', '2022_03_07',
                       '2022_03_08', '2022_03_09', '2022_03_10', '2022_03_11',
                       '2022_03_14', '2022_03_15', '2022_03_16', '2022_03_17',
                       '2022_03_18', '2022_03_21', '2022_03_22', '2022_03_23',
                       '2022_03_24', '2022_03_25', '2022_03_28', '2022_03_29',
                       '2022_03_30', '2022_03_31', '2022_04_01', '2022_04_04',
                       '2022_04_05', '2022_04_06', '2022_04_07', '2022_04_08',
                       '2022_04_12', '2022_04_13', '2022_04_14', '2022_04_15',
                       '2022_04_18', '2022_04_19', '2022_04_20', '2022_04_21',
                       '2022_04_22', '2022_04_25', '2022_04_26', '2022_04_27',
                       '2022_04_28', '2022_04_29', '2022_05_04', '2022_05_05',
                       '2022_05_06', '2022_05_09', '2022_05_10', '2022_05_11',
                       '2022_05_12', '2022_05_13', '2022_05_16', '2022_05_17',
                       '2022_05_18', '2022_05_19', '2022_05_20', '2022_05_23',
                       '2022_05_24', '2022_05_25', '2022_05_26', '2022_05_27',
                       '2022_05_30', '2022_05_31', '2022_06_01', '2022_06_02',
                       '2022_06_03', '2022_06_06', '2022_06_07', '2022_06_08',
                       '2022_06_09', '2022_06_10', '2022_06_13', '2022_06_14',
                       '2022_06_15', '2022_06_16', '2022_06_17', '2022_06_20',
                       '2022_06_21', '2022_06_22', '2022_06_23', '2022_06_24',
                       '2022_06_27', '2022_06_28', '2022_06_29', '2022_06_30',
                       '2022_07_01', '2022_07_04', '2022_07_05', '2022_07_06',
                       '2022_07_07', '2022_07_08', '2022_07_11', '2022_07_12',
                       '2022_07_13', '2022_07_14', '2022_07_15', '2022_07_18',
                       '2022_07_19', '2022_07_20', '2022_07_21', '2022_07_22',
                       '2022_07_25', '2022_07_26', '2022_07_27', '2022_07_28',
                       '2022_07_29', '2022_08_01', '2022_08_02', '2022_08_03',
                       '2022_08_04', '2022_08_05', '2022_08_08', '2022_08_09',
                       '2022_08_10', '2022_08_11', '2022_08_12', '2022_08_15',
                       '2022_08_16', '2022_08_17', '2022_08_18', '2022_08_19',
                       '2022_08_22', '2022_08_23', '2022_08_24', '2022_08_25',
                       '2022_08_26', '2022_08_29', '2022_08_30', '2022_08_31',
                       '2022_09_05', '2022_09_06', '2022_09_07', '2022_09_08',
                       '2022_09_09', '2022_09_12', '2022_09_13', '2022_09_14',
                       '2022_09_15', '2022_09_16', '2022_09_19', '2022_09_20',
                       '2022_09_21', '2022_09_22', '2022_09_23', '2022_09_26',
                       '2022_09_27', '2022_09_28', '2022_09_29', '2022_09_30',
                       '2022_10_03', '2022_10_04', '2022_10_05', '2022_10_06',
                       '2022_10_07', '2022_10_10', '2022_10_11', '2022_10_12',
                       '2022_10_13', '2022_10_14', '2022_10_17', '2022_10_18',
                       '2022_10_19', '2022_10_20', '2022_10_21', '2022_10_24',
                       '2022_10_25', '2022_10_26', '2022_10_27', '2022_10_28',
                       '2022_10_31', '2022_11_01', '2022_11_02', '2022_11_03',
                       '2022_11_04', '2022_11_07', '2022_11_08', '2022_11_09',
                       '2022_11_10', '2022_11_11', '2022_11_14', '2022_11_15',
                       '2022_11_16', '2022_11_17', '2022_11_18', '2022_11_21',
                       '2022_11_22', '2022_11_23', '2022_11_24', '2022_11_25',
                       '2022_11_28', '2022_11_29', '2022_11_30', '2022_12_01',
                       '2022_12_02', '2022_12_05', '2022_12_06', '2022_12_07',
                       '2022_12_08', '2022_12_09', '2022_12_12', '2022_12_13',
                       '2022_12_14', '2022_12_15', '2022_12_16', '2022_12_19',
                       '2022_12_20', '2022_12_21', '2022_12_22', '2022_12_23',
                       '2022_12_26', '2022_12_27', '2022_12_28', '2022_12_29',
                       '2022_12_30'
                       ]
        if max_day is None:
            max_day = dt.now().strftime('%Y_%m_%d')
        if max_day >= '2023_01_01':
            ALL_GD_DAYS += GD_DAYS.get_trading_days_2023()
        ALL_GD_DAYS = [x for x in ALL_GD_DAYS if min_day <= x <= max_day]

        return ALL_GD_DAYS

    @staticmethod
    def compute_missing_days(days, verbosity=1):
        import pandas as pd
        min_day, max_day = min(days), max(days)
        header = '\x1b[90mcompute_missing_days\x1b[0m: '
        if verbosity >= 2:
            print("{} \x1b[93m{}\x1b[0m days from {} => {}"
                .format(
                    header,
                    len(set(days)),
                    min_day, max_day))
        days = list(days)
        all_days = GD_DAYS.get_all_gd_days(min_day, max_day)
        df = pd.DataFrame(all_days)
        ffilter_has_not = ~df[0].isin(days)
        df_missing = df[ffilter_has_not].copy()
        missing_days = df_missing[0].tolist()
        return np.array(missing_days)


def get_vn30index_data_from_ws_redis():
    import pandas as pd
    from redis import StrictRedis
    from datetime import datetime as dt
    import json

    pd.options.display.max_columns = 25
    pd.options.display.max_rows = 50
    pd.options.display.width = 250
    ################################################################################
    day = dt.now().strftime('%Y_%m_%d')
    ################################################################################
    r = StrictRedis('ws', decode_responses=True)
    keys = [x for x in r.keys() if day in x if 'vn30' in x.lower()]

    key = '/d/data/kis_stream/2023_03_08_KIS.IndexAutoItem/VN30.txt'

    df = pd.DataFrame(map(lambda x: json.loads(x), r.lrange(key, 0, -1)))
    df = pd.concat([df, pd.DataFrame(df['data'].tolist())])
    del df['data']

    df['t'] = (df['timestamp'] / 1000).map(lambda x: dt.fromtimestamp(x) if x > 0 else x).fillna(method='ffill')
    #####
    dff = pd.DataFrame({'a': [1, 2]})
    dff.loc[0, 'b'] = 0
    my_nan = dff['b'].iloc[-1]
    # df['t'] = df['timestamp'].map(lambda x: dt.fromtimestamp(x / 1000) if x > 0 else x)
    # df[['t', 'timestamp']] = df[['t', 'timestamp']].fillna(method='ffill')
    # del df['timestamp'], df['ic']
    df = df.rename(columns={
        'c': 'vn30Index',
        'ch': 'change',
        'ra': 'changePct',
        'vo': 'volume',
        'va': 'value',
        'mv': 'matchingVolume'})
    df['value'] = df['value'] / 1000_000_000
    df = df[~df['vn30Index'].isna()]
    df['day'] = day
    df['time'] = df['ti'].map(str).map(
        lambda x: f'{int(x[:2]) + 7}'.rjust(2, '0') +
                  f':{x[2:4]}:{x[4:6]}')
    del df['ti']
    temp = df['t']
    del df['t']
    df['t'] = temp
    df['delay'] = df['t'].map(lambda x: x.timestamp()) \
                  - (df['day'] + ' ' + df['time']).map(lambda x:
                                                       dt.strptime(x, '%Y_%m_%d %H:%M:%S').timestamp()) \
                  - 3600 * 7

    return df


def kill_pydeconsole():
    import os

    dff = ps_aux_based_list_pids()
    lst = dff[dff['cmd']
    .map(lambda x: 'pydevconsole' in x)] \
        .to_dict('records')
    for dp in lst:
        print(f"{dp['cmd']} \x1b[93m{dp['pid']}\x1b[0m"
              .replace('pydevconsole.py',
                       '\x1b[96mpydevconsole.py\x1b[0m'))
        cmd = f'kill -9 {dp["pid"]}'
        if dp["pid"] == str(os.getpid()):
            cmd += ' \x1b[91m***THIS process**\x1b[0m'
        print(f"\x1b[94m{cmd}")
        if not dp["pid"] == str(os.getpid()):
            execute_cmd(cmd)
    _ = ps_aux_based_list_pids()



class VIEWER:
    PLOT_REDIS_HOST = 'ws'
    PLOT_REDIS_KEY = 'redis.channel.mac_local_plots'
    PLOT_DIR = '/tmp'
    SHOW = True

    @staticmethod
    def overwrite_fig_show():
        from redis import StrictRedis
        from plotly import express as px
        import pickle
        import zlib

        r = StrictRedis(VIEWER.PLOT_REDIS_HOST)
        def show_plot(fig, fn='plot'):
            message = {
                'html': zlib.compress(pickle.dumps(fig)),
                'fn': f'{VIEWER.PLOT_DIR}/{fn}.html',
                'show': VIEWER.SHOW,
            }

            r.publish(VIEWER.PLOT_REDIS_KEY,
                      pickle.dumps(message))

        fig_class = type(px.line(x=[0, 1], y=[1, 2]))
        fig_class.show = show_plot

    @staticmethod
    def subscribe(channel):
        from bokeh.util.browser import view
        from redis import StrictRedis
        import pickle
        import zlib

        r = StrictRedis(VIEWER.PLOT_REDIS_HOST)

        pubsub = r.pubsub()
        pubsub.subscribe(channel)
        print(f"Subscribed to channel: {channel}")

        # Listening for new messages
        for mess in pubsub.listen():
            if mess['type'] == 'message':
                try:
                    # noinspection PyGlobalUndefined
                    global msg
                    msg = pickle.loads(mess['data'])
                    fig = pickle.loads(zlib.decompress(msg['html']))
                    print(f"Received message show={msg['show']}")

                    try:
                        fig.write_html(msg['fn'])
                    except Exception as e:
                        print(f'\x1b[91m{e}\x1b[0m')
                        the_dir = '/'.join(msg['fn'].split('/')[:-1])
                        maybe_create_dir(the_dir)
                        fig.write_html(msg['fn'])

                    pickle_fn = f"{msg['fn']}.pickle"
                    with open(pickle_fn, 'wb') as file: pickle.dump(fig, file)
                    if msg['show']:
                        # with open(msg['fn'], 'w') as file:
                        #     file.write(fig)
                        view(msg['fn'])

                    print(f"\n\x1b[95m{dt.now().strftime('%H:%M:%S')}\x1b[0m: {pickle_fn}")

                except Exception as e:
                    report_error(e)
                    print(f"\n\x1b[94m{e}\x1b[0m")

    @staticmethod
    def forked_subscribe():
        import threading

        # Creating a new thread for the subscriber
        subscriber_thread = threading.Thread(
            target=VIEWER.subscribe,
            args=(VIEWER.PLOT_REDIS_KEY,))
        subscriber_thread.start()

