import pandas as pd
import pickle
import pyarrow.parquet as pq
from pathlib import Path
import os

class Utils:
    @staticmethod
    def and_conditions(conditions: list[pd.DataFrame]):
        """Calculate condition from orther conditions"""
        res = None
        for cond in conditions:
            if cond is not None:
                if res is None:
                    res = cond
                else:
                    res = res & cond 
        return res
    
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