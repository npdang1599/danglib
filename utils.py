# from dc_server.lazy_core import get_stock_daily
from danglib.Adapters.adapters import MongoDBs
import libtmux
from danglib.chatbots.viberbot import create_f5bot
from datetime import datetime as dt
import pandas as pd, numpy as np
import requests, os, sys
from datetime import date, timedelta
import pickle
import pyarrow.parquet as pq
from pathlib import Path
import json

# Disable
def blockPrint():
    sys.stdout = open(os.devnull, 'w')

# Restore
def enablePrint():
    sys.stdout = sys.__stdout__

def is_weekend(day):
    from datetime import datetime as dt
    date = dt.strptime(day, '%Y_%m_%d')
    return (date.weekday() == 5) | (date.weekday() == 6)

def get_maturity_dates(year):
    # year = 2022
    # third_thursdays = []
    dic_third_thursdays = {}
    dic_third_fridays = {}
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
        # third_thursdays.append(third_thursday)
        third_friday = third_thursday + timedelta(days=1)
        third_thursday = third_thursday.strftime('%Y_%m_%d')
        third_friday = third_friday.strftime("%Y_%m_%d")
        dic_third_thursdays[month] = third_thursday
        dic_third_fridays[month] = third_friday
    return dic_third_thursdays, dic_third_fridays

def get_previous_maturity_date(day):
    month = int(day[5:7])
    year = int(day[0:4])
    dic_third_thursdays = get_maturity_dates(year)[0]
    last_december_maturity_date = get_maturity_dates(year-1)[0][12]
    if day > dic_third_thursdays[month]:
        last_maturity_date = dic_third_thursdays[month]
    elif day <= dic_third_thursdays[1]:
        last_maturity_date = last_december_maturity_date
    else:
        last_maturity_date = dic_third_thursdays[month-1]
    return last_maturity_date

def get_ps_ticker(day):
    last_maturity_day = get_previous_maturity_date(day)
    month = int(last_maturity_day[5:7])
    year = int(last_maturity_day[0:4])
    if month == 12:
        F1_month = 1
        F1_year = year+1
    else:
        F1_month = month+1
        F1_year = year
    F2_month = F1_month+1
    if F2_month % 3 ==0:
        F3_month = F2_month+3
        F4_month = F2_month+6
    else:
        F3_month = F2_month//3 * 3 + 3
        F4_month = F3_month+3
    if F2_month > 12:
        F2_month = F2_month - 12
        F2_year = F1_year+1
    else:
        F2_year = F1_year
    if F3_month > 12:
        F3_month = F3_month - 12
        F3_year = F1_year+1
    else:
        F3_year = F1_year
    if F4_month > 12:
        F4_month = F4_month - 12
        F4_year = F1_year+1
    else:
        F4_year = F1_year

    F1_month = str(F1_month).zfill(2)
    F2_month = str(F2_month).zfill(2)
    F3_month = str(F3_month).zfill(2)
    F4_month = str(F4_month).zfill(2)

    F1_year = str(F1_year)[-2:]
    F2_year = str(F2_year)[-2:]
    F3_year = str(F3_year)[-2:]
    F4_year = str(F4_year)[-2:]

    return {'f1':f'VN30F{F1_year}{F1_month}',
            'f2':f'VN30F{F2_year}{F2_month}',
            'f3':f'VN30F{F3_year}{F3_month}',
            'f4':f'VN30F{F4_year}{F4_month}'}

def walk_through_files(TARGET, ext="*"):
    import os
    lst = []
    for root, dirs, files in os.walk(TARGET):
        for file in files:
            if ext == "*" or file.endswith(ext):
                lst.append(os.path.join(root, file))

    return lst

class FileHandler:
    @staticmethod
    def isExists(file_path):
        return Path(file_path).is_file()
    
    @staticmethod
    def walk_through_files(TARGET, ext="*"):
        lst = []
        for root, dirs, files in os.walk(TARGET):
            for file in files:
                if ext == "*" or file.endswith(ext):
                    lst.append(os.path.join(root, file))

        return lst
    
    @staticmethod
    def delete(file_path):
        try:
            path = Path(file_path)
            path.unlink()
            print(f"File deleted: {file_path}")
        except FileNotFoundError:
            print("File does not exist.")
        except PermissionError:
            print("Permission denied to delete the file.")
        except Exception as e:
            print(f"An error occurred: {e}")

    @staticmethod
    def maybe_create_dir(path, verbose=0):
        if not os.path.exists(path):
            os.makedirs(path)
            if verbose > 0:
                print(f"Created folder {DIR}")

    @staticmethod
    def write_pickle(path, data):
        with open(path, 'wb') as file:
            pickle.dump(data, file)
            
    @staticmethod
    def read_pickle(path):
        with open(path, 'rb') as file:
            data = pickle.load(file)
        return data
    
    @staticmethod
    def write_parquet(file_path, df: pd.DataFrame):
        """Write data to parquet file using pandas"""
        df.to_parquet(file_path, compression='snappy')

    @staticmethod
    def read_parquet(file_path, columns=None):
        """Read data from parquet file using pandas"""
        df = pd.read_parquet(file_path, columns=columns)
        return df
    
    @staticmethod
    def get_parquet_columns(path, substrs: list=None):
        parquet_file = pq.ParquetFile(path)
        all_columns = parquet_file.schema.names
        if substrs is None:
            return all_columns
        if isinstance(substrs, str):
            substrs = [substrs]
        return [col for col in all_columns if any(substr in col for substr in substrs)]
    
    @staticmethod
    def load_json(file_path):
        with open(file_path, 'r') as f:
            data = json.load(f)
        return data
    
    @staticmethod
    def save_json(file_path, data):
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)    
        return file_path


def get_stock_daily(symbol='HPG', start_day=None, end_day=None, resolution='1D'):

    TIME_GAP = 34128060  # 395 days
    timestamp = int(dt.strptime(dt.now().strftime('%Y_%m_%d'), '%Y_%m_%d').timestamp())

    def str_to_stamp(day):
        return int(dt.strptime(day, '%Y_%m_%d').timestamp())

    if end_day is None and start_day is None:
        end_day = str(timestamp)
        start_day = str(timestamp - TIME_GAP)
    elif end_day is None:
        end_day = str(timestamp)
        start_day = str(str_to_stamp(start_day))
    elif start_day is None:
        start_day = str(str_to_stamp(end_day) - TIME_GAP)
    else:
        start_day = str(str_to_stamp(start_day))
        end_day = str(str_to_stamp(end_day))

    if start_day >= end_day:
        return 

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
    
    params = (
        ('symbol', symbol),
        ('resolution', resolution),
        ('from', start_day),
        ('to', end_day),
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

def divide_lst_to_blocks(target_lst, block_len):
        block_ls = [target_lst[i:i+block_len] for i in range(0, len(target_lst), block_len)]
        return block_ls

def get_traday_from_weights(start_day=None, end_day=None, equal=True, astype_int=False):
    e = 'e' if equal else ''
    query = {}
    if start_day is not None:
        query['day'] = {f'$gt{e}':start_day}
    if end_day is not None:
        if len(query) == 0:
            query['day'] = {f'$lt{e}':end_day}
        else:
            query['day'][f'$lt{e}'] = end_day

    db = MongoDBs.ws_21_weights()
    col = db['vn30_weights']
    try:
        tradays = sorted(set(i['day'] for i in col.find(query,{"_id":0, "day":1})))
    except:
        tradays = []

    if astype_int:
        tradays = [int(i) for i in tradays]
    return tradays

def get_traday_from_gsd(start_day=None, end_day=None):
    tradays = get_stock_daily('HPG', start_day, end_day)['day'].to_list()

    def isin_day_range(x):
        if start_day is not None and end_day is not None:
            return x >= start_day and x <= end_day
        elif start_day is not None:
            return x >= start_day
        elif end_day is not None:
            return x <= end_day
        else:
            return True

    tradays = sorted(set(i for i in tradays if isin_day_range(i)))
    return tradays


@staticmethod
def flatten_columns(df: pd.DataFrame, separator='_'):
    """
    Làm phẳng MultiIndex columns của DataFrame thành single-level columns.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame có MultiIndex columns cần làm phẳng
    separator : str, default='_'
        Ký tự để nối các level của columns
        
    Returns:
    --------
    pandas.DataFrame
        DataFrame mới với columns đã được làm phẳng
    """
    
    # Kiểm tra nếu columns không phải MultiIndex thì return nguyên DataFrame
    if not isinstance(df.columns, pd.MultiIndex):
        return df
    
    # Tạo tên mới cho columns bằng cách nối các level
    flat_columns = []
    for col in df.columns:
        # Loại bỏ các giá trị rỗng và khoảng trắng
        cleaned_col = [str(level).strip() for level in col if pd.notna(level) and str(level).strip() != '']
        # Nối các level bằng separator
        new_col = separator.join(cleaned_col)
        flat_columns.append(new_col)
    
    # Tạo DataFrame mới với columns đã làm phẳng
    df_flat = df.copy()
    df_flat.columns = flat_columns
    
    return df_flat

@staticmethod
def unflatten_columns(df: pd.DataFrame, separator='_', level_names=None):
    """
    Chuyển đổi columns đã được làm phẳng thành MultiIndex columns.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame có columns đã được làm phẳng cần chuyển về MultiIndex
    separator : str, default='_'
        Ký tự đã dùng để ngăn cách các level trong tên cột
    level_names : list, default=None
        Tên cho các level của MultiIndex. Nếu None, sẽ dùng số thứ tự
        
    Returns:
    --------
    pandas.DataFrame
        DataFrame với columns đã được chuyển thành MultiIndex
    """
    # Tách các columns thành các phần theo separator
    split_cols = [col.split(separator) for col in df.columns]
    
    # Xác định số lượng levels
    max_levels = max(len(parts) for parts in split_cols)
    
    # Chuẩn hóa độ dài của tất cả các phần
    normalized_cols = []
    for parts in split_cols:
        # Nếu thiếu level thì thêm chuỗi rỗng vào
        if len(parts) < max_levels:
            parts.extend([''] * (max_levels - len(parts)))
        normalized_cols.append(tuple(parts))
    
    # Tạo tên cho các level nếu không được cung cấp
    if level_names is None:
        level_names = [f'level_{i}' for i in range(max_levels)]
    elif len(level_names) < max_levels:
        # Nếu thiếu tên level thì thêm vào
        level_names.extend([f'level_{i}' for i in range(len(level_names), max_levels)])
    
    # Tạo MultiIndex mới
    df_multi = df.copy()
    df_multi.columns = pd.MultiIndex.from_tuples(
        normalized_cols,
        names=level_names
    )
    
    return df_multi



class Tmuxlib5014:


    @staticmethod
    def init_session(session_name):
        server = libtmux.Server()
        session: libtmux.session = server.find_where({'session_name': session_name})
        return session

    @staticmethod
    def terminate_pane_process(pane: libtmux.Pane):
        pane.send_keys("C-c")

    @staticmethod
    def ternimate_and_run_cmd_pane(pane: libtmux.Pane, cmd:str, start_dir:str=None, conda_env=None):
        Tmuxlib5014.terminate_pane_process(pane)
        if start_dir is not None:
            pane.send_keys(f"cd {start_dir}")
        if conda_env is not None:
            pane.send_keys(f"conda activate {conda_env}")
        pane.send_keys(f"alias cmd='{cmd}'")
        pane.send_keys("cmd")

    @staticmethod
    def check_right_window_format(window: libtmux.Window, n_panes, idx_list):

        is_unchange_npane = len(window.panes) == n_panes
        is_unchange_size = all([idx in idx_list for idx in [p.pane_id for p in window.panes] ])
        return all(
                [is_unchange_npane]
            )

    @staticmethod
    def restart_db22_worker():
        F5bot = create_f5bot()
        ss = Tmuxlib5014.init_session(session_name='5104')
        wd:libtmux.Window = ss.windows[0]

        wd_f = {
            'n_panes': 2,
            'panes_size': ['%0', '%78', '%42', '%77']
        }

        right_f = Tmuxlib5014.check_right_window_format(wd, n_panes=wd_f['n_panes'], idx_list=wd_f['panes_size'])
        if right_f:
            pane: libtmux.Pane = wd.panes[1]
            Tmuxlib5014.ternimate_and_run_cmd_pane(pane, 
                start_dir= "/home/ubuntu/anaconda3/lib/python3.8/dc_server/db22",
                cmd='export PYTHONHASHSEED=0; celery -A db22_worker worker --pool solo --loglevel=INFO -n worker_db22_1@mx')
            F5bot.send_viber(msg="db11 5104 had restarted!")
        else:
            F5bot.send_viber(msg="db11 5104 tmux had change format")

    @staticmethod
    def restart_5104_server():
        F5bot = create_f5bot()
        ss = Tmuxlib5014.init_session(session_name='5104')
        wd: libtmux.Window = ss.windows[0]

        pane: libtmux.Pane = wd.panes[0]
        Tmuxlib5014.ternimate_and_run_cmd_pane(pane,
                start_dir = "/home/ubuntu/db11_5104",
                cmd='python db11_5104_dev.py',
                conda_env="rapids3"
            )
        
        F5bot.send_viber(msg="server 5104 had restarted!")

def maybe_create_dir(path, verbose=0):
    if not os.path.exists(path):
        os.makedirs(path)
        if verbose > 0:
            print(f"Created folder {path}")


def execute_cmd(cmd: str, print_result=True, wait_til_finish=True):
    import subprocess, re, shlex
    cmd = cmd.replace("\n", "").strip().replace("\t", "")
    cmd = cmd.replace("\n", "").strip().replace("\t", "")
    cmd = re.sub(" +", " ", cmd)
    args = shlex.split(cmd)
    process = subprocess.Popen(args, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if not wait_til_finish: return process
    stdout, stderr = process.communicate()
    err = stderr.decode("utf-8")
    if len(err) > 0: print(f"{err}")
    res = stdout.decode("utf-8")
    if print_result: print(f"{res}")
    return re.sub(" +", " ", res)

def write_pickle(path, data):
    with open(path, 'wb') as file:
        pickle.dump(data, file)
        
def read_pickle(path):
    with open(path, 'rb') as file:
        data = pickle.load(file)
    return data
    
def show_ram_usage_mb():
    import os
    import psutil
    pid = os.getpid()
    python_process = psutil.Process(pid)
    memoryUse = round(python_process.memory_info()[0] / 2. ** 20, 2) # memory use in GB
    return memoryUse


def check_column_sorted(src: pd.Series, ascending=True):
    """
    Kiểm tra xem một cột trong DataFrame đã được sắp xếp hay chưa
    
    Parameters:
    src (pandas.Series): Series cần kiểm tra
    column_name (str): Tên cột cần kiểm tra
    ascending (bool): True nếu kiểm tra sắp xếp tăng dần, False nếu giảm dần
    
    Returns:
    bool: True nếu cột đã được sắp xếp, False nếu chưa
    dict: Thông tin chi tiết về việc sắp xếp
    """
    
    
    # Lấy giá trị của cột
    column_values = src.values
    
    # Bỏ qua các giá trị NaN
    valid_values = column_values[~pd.isna(column_values)]
    
    # So sánh với phiên bản đã sắp xếp
    sorted_values = np.sort(valid_values)
    if not ascending:
        sorted_values = sorted_values[::-1]
    
    is_sorted = np.array_equal(valid_values, sorted_values)
    
    # Tạo thông tin chi tiết
    details = {
        'is_sorted': is_sorted,
        'direction': 'tăng dần' if ascending else 'giảm dần',
        'total_values': len(valid_values),
        'nan_count': len(column_values) - len(valid_values)
    }
    
    if not is_sorted:
        # Tìm vị trí đầu tiên không theo thứ tự
        for i in range(len(valid_values)-1):
            if ascending:
                if valid_values[i] > valid_values[i+1]:
                    details['first_unsorted_index'] = i
                    details['unsorted_values'] = (valid_values[i], valid_values[i+1])
                    break
            else:
                if valid_values[i] < valid_values[i+1]:
                    details['first_unsorted_index'] = i
                    details['unsorted_values'] = (valid_values[i], valid_values[i+1])
                    break
    
    return is_sorted, details

def day_to_timestamp(day: str):
    return int(pd.to_datetime(day, format='%Y_%m_%d').timestamp()) * 1000000000


def unflatten_columns(df: pd.DataFrame, separator='_', level_names=None):
    """
    Chuyển đổi columns đã được làm phẳng thành MultiIndex columns.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame có columns đã được làm phẳng cần chuyển về MultiIndex
    separator : str, default='_'
        Ký tự đã dùng để ngăn cách các level trong tên cột
    level_names : list, default=None
        Tên cho các level của MultiIndex. Nếu None, sẽ dùng số thứ tự
        
    Returns:
    --------
    pandas.DataFrame
        DataFrame với columns đã được chuyển thành MultiIndex
    """
    # Tách các columns thành các phần theo separator
    split_cols = [col.split(separator) for col in df.columns]
    
    # Xác định số lượng levels
    max_levels = max(len(parts) for parts in split_cols)
    
    # Chuẩn hóa độ dài của tất cả các phần
    normalized_cols = []
    for parts in split_cols:
        # Nếu thiếu level thì thêm chuỗi rỗng vào
        if len(parts) < max_levels:
            parts.extend([''] * (max_levels - len(parts)))
        normalized_cols.append(tuple(parts))
    
    # Tạo tên cho các level nếu không được cung cấp
    if level_names is None:
        level_names = [f'level_{i}' for i in range(max_levels)]
    elif len(level_names) < max_levels:
        # Nếu thiếu tên level thì thêm vào
        level_names.extend([f'level_{i}' for i in range(len(level_names), max_levels)])
    
    # Tạo MultiIndex mới
    df_multi = df.copy()
    df_multi.columns = pd.MultiIndex.from_tuples(
        normalized_cols,
        names=level_names
    )
    
    return df_multi

def underscore_to_camel(text):
    """
    Chuyển đổi chuỗi từ dạng underscore sang CamelCase
    
    Args:
        text (str): Chuỗi đầu vào dạng underscore (ví dụ: hello_world)
        
    Returns:
        str: Chuỗi đã chuyển đổi sang CamelCase (ví dụ: HelloWorld)
        
    Examples:
        >>> underscore_to_camel('hello_world')
        'HelloWorld'
        >>> underscore_to_camel('user_first_name')
        'UserFirstName'
        >>> underscore_to_camel('api_response_data')
        'ApiResponseData'
    """
    # Tách chuỗi theo dấu gạch dưới và loại bỏ khoảng trắng
    words = text.strip().split('_')
    
    # Chuyển đổi chữ đầu tiên của mỗi từ thành chữ hoa
    return ''.join(word.capitalize() for word in words)

def check_run_with_interactive():
    import sys
    return any(
        arg in sys.argv[0] for arg in 
        ['ipykernel_launcher', 'ipykernel', '-c', 'jupyter', 'vscode']
    )

def totime(stamp, unit='s'):
    return pd.to_datetime(stamp, unit=unit)

def clean_plasma_key(key, db):
    from danglib.lazy_core import gen_plasma_functions
    _, disconnect, psave, pload = gen_plasma_functions(db)
    psave(key, None)
    disconnect()

def today():
    return dt.now().strftime('%Y_%m_%d')