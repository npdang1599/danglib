import pandas as pd
import pickle
import pyarrow.parquet as pq
from pathlib import Path
import os
from redis import StrictRedis
from datetime import timedelta
import hashlib
import random
from functools import reduce
import json
import numpy as np
from pandas import Interval

class RedisHandler:
    def __init__(self, host='localhost', port=6379, db=0):
        self.redis_client = StrictRedis(
            host=host,
            port=port,
            db=db,
            decode_responses=False  # Tự động decode response từ bytes sang string
        )

    def check_exist(self, key):
        return self.redis_client.exists(key)
    
    def set_key_with_ttl(self, key: str, value: str, pickle_data = False, force = False) -> bool:
        """
        Set key với giá trị và TTL 1 ngày nếu key chưa tồn tại
        Return: True nếu set thành công, False nếu key đã tồn tại
        """
        # Kiểm tra key đã tồn tại chưa
        if self.check_exist(key) and not force:
            return False

        if pickle_data:
            value = pickle.dumps(value)
        
        # Set key với TTL 1 ngày (86400 giây)
        self.redis_client.set(
            name=key,
            value=value,
            ex=timedelta(days=1)  # Tự động xoá sau 1 ngày
        )
        return True
    
    def get_key(self, key: str, pickle_data = False) -> str:
        """
        Lấy giá trị của key
        Return: Giá trị của key hoặc None nếu key không tồn tại
        """
        data = self.redis_client.get(key)
        if pickle_data:
            return pickle.loads(data)

        return data
    
    def list_keys_with_pattern(self, pattern: str) -> list:
        # Sử dụng scan_iter để lấy tất cả các key khớp với pattern
        matching_keys = list(self.redis_client.scan_iter(match=pattern))
        return matching_keys


    def delete_keys_by_pattern(self, pattern: str) -> int:
        """
        Xóa tất cả các key khớp với pattern cho trước
        
        Args:
            pattern (str): Pattern để tìm key, ví dụ: "user:*" sẽ khớp với tất cả key bắt đầu bằng "user:"
        
        Returns:
            int: Số lượng key đã xóa
        """

        matching_keys = self.list_keys_with_pattern(pattern)
        
        if not matching_keys:
            return 0
            
        # Xóa tất cả các key tìm được
        deleted_count = self.redis_client.delete(*matching_keys)
        return deleted_count
    
    @staticmethod
    def create_hash_key(original_string: str, prefix: str = None):

        if prefix is not None:
            prefix = f"{prefix}/"
        else:
            prefix = ""

        hashed_count_conditions = f"{prefix}{hash_sha256(original_string)}"
        return hashed_count_conditions


class Utils:
    @staticmethod
    def connect_to_mongo(host, port, db_name):
        from pymongo import MongoClient
        client = MongoClient(host=host, port=port)
        db = client[db_name]
        return db

    @staticmethod
    def and_conditions(conditions: list):
        # Lọc điều kiện None
        valid_conditions = [cond for cond in conditions if cond is not None]
        
        if not valid_conditions:
            return None
        
        if len(valid_conditions) == 1:
            return valid_conditions[0]
        
        # Xử lý cả DataFrame và Series
        def combine(obj1: pd.DataFrame, obj2: pd.DataFrame):
            obj1_aligned, obj2_aligned = obj1.align(obj2, fill_value=False)
            return obj1_aligned & obj2_aligned
        
        return reduce(combine, valid_conditions)
    
    @staticmethod
    def or_conditions(conditions: list[pd.DataFrame]):
        #Lọc điều kiện None
        valid_conditions = [cond for cond in conditions if cond is not None]
        
        if not valid_conditions:
            return None
        
        if len(valid_conditions) == 1:
            return valid_conditions[0]
        
        # Xử lý cả DataFrame và Series
        def combine(obj1, obj2):
            obj1_aligned, obj2_aligned = obj1.align(obj2, fill_value=False)
            return obj1_aligned | obj2_aligned
        
        return reduce(combine, valid_conditions)
    
    @staticmethod
    def day_to_timestamp(day: str, is_end_day = False):
        stamp = int(pd.to_datetime(day, format='%Y_%m_%d').timestamp())
        if is_end_day:
            stamp += 86400

        return stamp * 1000000000

    @staticmethod
    def compress_bool_series(series: pd.Series) -> bytearray:
        """Nén pandas Series boolean thành dạng bytearray.
        
        Args:
            series (pd.Series): Series boolean cần nén
                
        Returns:
            bytearray: Dữ liệu đã được nén
        """
        bits = series.astype(int).values
        result = bytearray((len(bits) + 7) // 8)  # Pre-allocate đúng size
        
        # Nén 8 bits vào 1 byte
        for i, bit in enumerate(bits):
            if bit:  # Chỉ set bit khi giá trị là True
                result[i // 8] |= (1 << (i % 8))
                
        return result

    @staticmethod
    def decompress_to_bool_list(byte_array: bytearray, length: int) -> list:
        """Giải nén bytearray thành list boolean.
        
        Args:
            byte_array (bytearray): Dữ liệu đã nén cần giải nén
            length (int): Độ dài của series boolean gốc
                
        Returns:
            list: List các giá trị boolean
        """
        result = []
        
        for i in range(length):
            byte_index = i // 8
            bit_index = i % 8
            result.append(bool(byte_array[byte_index] & (1 << bit_index)))
            
        return result
    
    @staticmethod
    def convert_bool_series_to_integer(series: pd.Series) -> int:
        """Chuyển đổi pandas Series boolean thành một số nguyên sử dụng bitwise operations.
        
        Args:
            series (pd.Series): Series boolean cần chuyển đổi
            
        Returns:
            int: Số nguyên chứa thông tin của series boolean
        """
        # Chuyển series thành list các bit (0/1)
        bits = series.astype(int).values
        
        # Chuyển list bit thành một số nguyên
        result = 0
        for i in range(len(bits)):
            result |= bits[i] << i
            
        return result

    @staticmethod
    def convert_integer_to_bool_list(integer: int, length: int) -> list:
        """Chuyển đổi số nguyên trở lại thành list boolean.
        
        Args:
            integer (int): Số nguyên cần giải nén
            length (int): Độ dài của series boolean gốc
            
        Returns:
            list: List các giá trị boolean
        """
        return [(integer >> i) & 1 == 1 for i in range(length)]
        
    @staticmethod
    def align_indexes(target: pd.Series, source: pd.Series) -> pd.Series:
        """Align index của target với source, fill_value là False"""
        target = target.copy()
        if not target.index.equals(source.index):
            target = target.reindex(source.index, fill_value=False)
        return target

    
    @staticmethod
    def convert_timeframe_to_rolling(timeframe: str) -> int:
        """Convert timeframe string to number of 30-second periods."""
        number = int(''.join(filter(str.isdigit, timeframe)))
        unit = ''.join(filter(str.isalpha, timeframe))
        
        seconds = 0
        if unit == 'S':
            seconds = number
        elif unit == 'Min':
            seconds = number * 60
        elif unit == 'H':
            seconds = number * 3600
        elif unit == 'D':
            seconds = number * 86400
        
        return seconds // 30
    
    @staticmethod
    def new_1val_series(value, series_to_copy_index: pd.Series):
        """Create an one-value series replicated another series' index"""
        return pd.Series(value, index=series_to_copy_index.index)
    
    @staticmethod
    def new_1val_df(value, df_sample: pd.DataFrame):
        """Create an one-value dataframe"""
        return pd.DataFrame(value, columns=df_sample.columns, index=df_sample.index)
    
    @staticmethod
    def new_1val_pdObj(value, pdObj_sample):
        if isinstance(pdObj_sample, pd.Series):
            return Utils.new_1val_series(value, pdObj_sample)
        elif isinstance(pdObj_sample, pd.DataFrame):
            return Utils.new_1val_df(value, pdObj_sample)
        else:
            raise ValueError("pdObj_sample must be either pd.Series or pd.DataFrame")
        

    @staticmethod
    def random_color():
        """Trả về một màu ngẫu nhiên dưới dạng mã hex"""
        return '#{:06x}'.format(random.randint(0, 0xFFFFFF))



class FileHandler:

    class CustomJSONEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, Interval):
                return str(obj)
            if isinstance(obj, np.integer):
                return int(obj)
            if isinstance(obj, np.floating):
                return float(obj)
            if isinstance(obj, np.ndarray):
                return obj.tolist()
            if isinstance(obj, np.bool_):  # Handle numpy boolean specifically
                return bool(obj)
            return super().default(obj)

    @staticmethod
    def get_file_size_mb(file_path):
        """
        Hàm kiểm tra kích thước của file và trả về kết quả bằng MB
        
        Tham số:
            file_path (str): Đường dẫn đến file cần kiểm tra
            
        Trả về:
            float: Kích thước của file tính bằng MB
            None: Nếu file không tồn tại hoặc có lỗi
        """
        try:
            # Kiểm tra xem file có tồn tại không
            if not os.path.isfile(file_path):
                print(f"Lỗi: File '{file_path}' không tồn tại")
                return None
                
            # Lấy kích thước file bằng byte
            file_size_bytes = os.path.getsize(file_path)
            
            # Chuyển đổi sang MB (1 MB = 1024 * 1024 bytes)
            file_size_mb = file_size_bytes / (1024 * 1024)
            
            return file_size_mb
            
        except Exception as e:
            print(f"Lỗi khi kiểm tra kích thước file: {e}")
            return None

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
                print(f"Created folder {path}")

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
    def read_parquet(file_path):
        """Read data from parquet file using pandas"""
        df = pd.read_parquet(file_path)
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
    def read_json(path):
        with open(path, 'r') as file:
            data = json.load(file)
        return data
    
    @staticmethod
    def write_json(path, data, use_custom_encoder=False):
        custom_encoder = FileHandler.CustomJSONEncoder if use_custom_encoder else None
        with open(path, 'w') as file:
            json.dump(data, file, cls=custom_encoder, indent=4)
            
    @staticmethod
    def read_jsonl(path):
        with open(path, 'r') as file:
            data = [json.loads(line) for line in file]
        return data
    
    @staticmethod
    def write_jsonl(path, data, use_custom_encoder=False):
        custom_encoder = FileHandler.CustomJSONEncoder if use_custom_encoder else None

        with open(path, 'w') as file:
            for item in data:
                file.write(json.dumps(item, cls=custom_encoder) + '\n')


def day_to_timestamp(day: str, is_end_day = False):
    stamp = int(pd.to_datetime(day, format='%Y_%m_%d').timestamp())
    if is_end_day:
        stamp += 86400

    return stamp * 1000000000


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


def hash_sha256(value):
    # Chuyển thành bytes nếu đầu vào là chuỗi
    value_bytes = value.encode('utf-8') if isinstance(value, str) else value
    # Tính hash bằng SHA-256
    hash_object = hashlib.sha256(value_bytes)
    return hash_object.hexdigest()  # Trả về dạng chuỗi hex


def generate_candle_times(timeframe: str, day: str=None, start_time='09:15:00', end_time='14:45:00', to_timestamp=False, unit='ns'):
    """
    Tạo ra Series các mốc thời gian cho nến dựa trên timeframe
    
    Parameters:
    timeframe (str): Khung thời gian ('1s', '5s', '15s', '30s', '1min', '5min', '15min', '30min', '1h', '4h', '1d')
    day (str): Ngày (format: 'YYYY_MM_DD')
    start_time (str): Thời gian bắt đầu (format: 'HH:MM:SS'), mặc định '09:15:00'
    end_time (str): Thời gian kết thúc (format: 'HH:MM:SS'), mặc định '14:45:00'
    
    Returns:
    pd.Series: Series chứa các mốc thời gian cho nến
    """
    timeframe = timeframe.replace('S', 's')
    start_time_full = day.replace('_', '-') + ' ' + start_time
    lunch_time = day.replace('_', '-') + ' ' + '11:30:00'
    
    afternoon_time = day.replace('_', '-') + ' ' + '13:00:00'
    atc_time = day.replace('_', '-') + ' ' + '14:30:00'
    end_time_full = day.replace('_', '-') + ' ' + end_time
    
    start_dt = pd.to_datetime(start_time_full)
    end_dt = pd.to_datetime(end_time_full)
    
    candle_times = pd.Series(pd.date_range(start=start_dt, end=end_dt, freq=timeframe))
    
    # Morning session: 9:15:00 - 11:30:00
    filter_morning = (candle_times < lunch_time)
    
    # Afternoon session: 13:00:00 - 14:30:00
    filter_afternoon = (candle_times >= afternoon_time) & (candle_times < atc_time)

    # ATC
    filter_atc = (candle_times == end_dt)

    candle_times = candle_times[filter_morning|filter_afternoon|filter_atc]
    if to_timestamp:
        candle_times = candle_times.astype(int)

        if unit=='s':
            candle_times = candle_times // 1e9
        elif unit=='ms':
            candle_times = candle_times // 1e6

        candle_times = candle_times.astype(int)

    return candle_times.reset_index(drop=True)

def totime(stamp, unit='s'):
    return pd.to_datetime(stamp, unit=unit)