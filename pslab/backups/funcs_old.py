from typing import Dict, Union
import pandas as pd
import numpy as np

from danglib.pslab.resources import Adapters, Globs, RedisHandler
from danglib.pslab.utils import Utils

from typing import Union, List, Dict
from dataclasses import dataclass
from copy import deepcopy

class NoRedisDataError(Exception):
    """Raised when required Redis data is not found"""
    pass

class InputSourceEmptyError(Exception):
    """Raise when Input sources are empty"""
    pass

PandasObject = Union[pd.Series, pd.DataFrame]

def remove_nan_keys(data):
    """Xoá tất cả key có value bằng NaN trong từng object của list"""
    return [{key: value for key, value in obj.items() if not np.isnan(value)} for obj in data]

def append_to_file(filename: str, content: str) -> None:
    """
    Append content to a file. If the file doesn't exist, it will be created.
    
    Args:
        filename (str): The path to the file
        content (str): The content to append to the file
    
    Returns:
        None
    
    Example:
        append_to_file("example.txt", "Hello World\n")
    """
    try:
        # Open file in append mode ('a')
        # If file doesn't exist, it will be created
        with open(filename, 'a', encoding='utf-8') as file:
            file.write(content)
    except IOError as e:
        print(f"Error occurred while writing to file: {e}")

def function_mapping():
    """
    Maps function names to their corresponding function objects and descriptive titles.
    
    Returns:
        dict: Dictionary with function names as keys, each mapping to a dict containing:
            - 'function': The actual function object
            - 'title': A human-readable title describing the function
            - 'inputs': List of required input parameters
            - 'params': Dictionary of additional parameters with their types and defaults
    """
    return {
        'absolute_change_in_range': {   
            'function': Conds.absolute_change_in_range,
            'title': 'Absolute Change in Range',
            'description': "sự thay đổi giá trị tuyệt đối trong một khoảng thời gian",
            'inputs': ['src'],
            'params': {
                'n_bars': {'type': 'int', 'default': 1},
                'lower_thres': {'type': 'float', 'default': -999},
                'upper_thres': {'type': 'float', 'default': 999},
                "use_as_lookback_cond" : {'type': 'bool', 'default': False},
                'lookback_cond_nbar' : {'type': 'int', 'default': 5}
            }
        },
        'consecutive_above_below': {
            'function': Conds.consecutive_above_below,
            'title': 'Consecutive Above/Below',
            'description': "một đường nằm trên/dưới đường khác liên tiếp",
            'inputs': ['src1', 'src2'],
            'params': {
                'direction': {'type': 'str', 'default': 'above', 'values': ['above', 'below']},
                'num_bars': {'type': 'int', 'default': 5},
                "use_as_lookback_cond" : {'type': 'bool', 'default': False},
                'lookback_cond_nbar' : {'type': 'int', 'default': 5}
            }
        },
        'cross_no_reverse': {
            'function': Conds.cross_no_reverse,
            'title': 'Cross Without Reversal',
            'description': "Xác định giao cắt mà không đảo chiều sau đó",
            'inputs': ['src1', 'src2'],
            'params': {
                'direction': {'type': 'str', 'default': 'crossover', 'values': ['crossover', 'crossunder']},
                'bars_no_reverse': {'type': 'int', 'default': 5},
                "use_as_lookback_cond" : {'type': 'bool', 'default': False},
                'lookback_cond_nbar' : {'type': 'int', 'default': 5}
            }
        },
        'percentile_in_range': {
            'function': Conds.percentile_in_range,
            'title': 'Percentile in Range',
            'description': "Kiểm tra percentile của source trong khi lookback n bars có nằm trong range giá trị",
            'inputs': ['src'],
            'params': {
                'lookback_period': {'type': 'int', 'default': 20},
                'lower_thres': {'type': 'float', 'default': -999},
                'upper_thres': {'type': 'float', 'default': 999},
                "use_as_lookback_cond" : {'type': 'bool', 'default': False},
                'lookback_cond_nbar' : {'type': 'int', 'default': 5}
            },
        },
        'gap_percentile': {
            'function': Conds.gap_percentile,
            'title': 'Gap Percentile',
            'description': "Kiểm tra khoảng cách giữa hai đường thuộc nhóm phần trăm cao nhất",
            'inputs': ['src1', 'src2'],
            'params': {
                'lookback_period': {'type': 'int', 'default': 20},
                'threshold': {'type': 'float', 'default': 90},
                "use_as_lookback_cond" : {'type': 'bool', 'default': False},
                'lookback_cond_nbar' : {'type': 'int', 'default': 5}
            }
        },
        'gap_trend': {
            'function': Conds.gap_trend,
            'title': 'Gap Trend Analysis',
            'description': "Phân tích xu hướng tăng/giảm của gap giữa hai đường",
            'inputs': ['src1', 'src2'],
            'params': {
                'sign': {'type': 'str', 'default': 'positive', 'values': ['positive', 'negative']},
                'trend_direction': {'type': 'str', 'default': 'increase', 'values': ['increase', 'decrease']},
                'trend_bars': {'type': 'int', 'default': 5, 'des':"Số thanh nến để xác định xu hướng"},
                "use_as_lookback_cond" : {'type': 'bool', 'default': False},
                'lookback_cond_nbar' : {'type': 'int', 'default': 5}
            }
        },
        'gap_range':{
            'function': Conds.gap_range,
            'title': 'Gap Range',
            'description': "Kiểm tra khoảng cách giữa hai đường nằm trong một khoảng xác định",
            'inputs': ['src1', 'src2'],
            'params': {
                'lower_thres': {'type': 'float', 'default': -999},
                'upper_thres': {'type': 'float', 'default': 999},
                "use_as_lookback_cond" : {'type': 'bool', 'default': False},
                'lookback_cond_nbar' : {'type': 'int', 'default': 5}
            }
        },
        'is_in_top_bot_percentile': {
            'function': Conds.is_in_top_bot_percentile,
            'title': 'Top/Bottom Percentile',
            'description': "Xác định giá trị thuộc nhóm cao nhất hoặc thấp nhất",
            'inputs': ['src'],
            'params': {
                'lookback_period': {'type': 'int', 'default': 20},
                'direction': {'type': 'str', 'default': 'top', 'values': ['top', 'bottom']},
                'threshold': {'type': 'float', 'default': 90},
                "use_as_lookback_cond" : {'type': 'bool', 'default': False},
                'lookback_cond_nbar' : {'type': 'int', 'default': 5}
            }
        },
        'min_inc_dec_bars': {
            'function': Conds.min_inc_dec_bars,
            'title': 'Minimum Increasing/Decreasing Bars',
            'description': "Đếm số thanh nến tăng/giảm tối thiểu",
            'inputs': ['src'],
            'params': {
                'n_bars': {'type': 'int', 'default': 10},
                'n_bars_inc': {'type': 'int', 'default': 10, 'des': 'Số thanh nến tăng'},
                'n_bars_dec': {'type': 'int', 'default': 0, 'des': 'Số thanh nến giảm'},
                "use_as_lookback_cond" : {'type': 'bool', 'default': False},
                'lookback_cond_nbar' : {'type': 'int', 'default': 5}
            }
        },
        'percent_change_in_range': {
            'function': Conds.percent_change_in_range,
            'title': 'Percent Change in Range',
            'description': "Phân tích phần trăm thay đổi giá trị trong khoảng thời gian",
            'inputs': ['src'],
            'params': {
                'n_bars': {'type': 'int', 'default': 1},
                'lower_thres': {'type': 'float', 'default': 0},
                'upper_thres': {'type': 'float', 'default': 100},
                "use_as_lookback_cond" : {'type': 'bool', 'default': False},
                'lookback_cond_nbar' : {'type': 'int', 'default': 5}
            }
        },
        'range_nbars': {
            'function': Conds.range_nbars,
            'title': 'Total value over n bars in Range',
            'description': " Kiểm tra tổng giá trị của n bars có nằm trong một khoảng xác định",
            'inputs': ['src'],
            'params': {
                'lower_thres': {'type': 'float', 'default': -999},
                'upper_thres': {'type': 'float', 'default': 999},
                'sum_nbars' : {'type': 'int', 'default': 1},
                "use_as_lookback_cond" : {'type': 'bool', 'default': False},
                'lookback_cond_nbar' : {'type': 'int', 'default': 5}
            }
        },
        'range_cond': {
            'function': Conds.range_cond,
            'title': 'Value in Range',
            'description': "Kiểm tra giá trị có nằm trong một khoảng xác định",
            'inputs': ['src'],
            'params': {
                'lower_thres': {'type': 'float', 'default': -999},
                'upper_thres': {'type': 'float', 'default': 999},
                "use_as_lookback_cond" : {'type': 'bool', 'default': False},
                'lookback_cond_nbar' : {'type': 'int', 'default': 5}
            }
        },
        'two_line_pos': {
            'function': Conds.two_line_pos,
            'title': 'Two Line Position',
            'description': "Kiểm tra vị trí giữa hai đường",
            'inputs': ['src1', 'src2'],
            'params': {
                'direction': {'type': 'str', 'default': 'crossover', 'values': ['crossover', 'crossunder', 'above', 'below']},
                'equal': {'type': 'bool', 'default': False, 'description': 'Có xem xét giá trị bằng nhau không, ví dụ option src1 crossover src2, mà equal = True thì khi src1 = src2 cũng được xem là crossover, tương tự với crossunder, above, below'},
                "use_as_lookback_cond" : {'type': 'bool', 'default': False},
                'lookback_cond_nbar' : {'type': 'int', 'default': 5}
            }
        },
        'consecutive_squeeze': {
            'function': Conds.Indicators.consecutive_squeezes,
            'title': 'Consecutive Squeezes',
            'description': "Kiểm tra số lượng squeeze liên tiếp",
            'inputs': ['src', 'high', 'low', 'close'],
            'params': {
                'bb_length': {'type': 'int', 'default': 20},
                'length_kc': {'type': 'int', 'default': 20},
                'mult_kc': {'type': 'float', 'default': 1.5},
                'num_bars_sqz': {'type': 'int', 'default': 5, 'description': 'Số thanh nến squeeze'},
                'use_no_sqz': {'type': 'bool', 'default': False, 'description': "Kiểm tra không có squeeze"},
                'use_as_lookback_cond': {'type': 'bool', 'default': False},
                'lookback_cond_nbar': {'type': 'int', 'default': 5},
            }
        },
        'two_MA_pos': {
            'function': Conds.Indicators.two_MA_pos,
            'title': 'Two MA Line Position',
            'description': "Kiểm tra vị trí giữa hai moving averages",
            'inputs': ['src'],
            'params': {
                'ma1': {'type': 'int', 'default': 5},
                'ma2': {'type': 'int', 'default': 15},
                'ma_type': {'type': 'str', 'default': 'SMA', 'values': ['SMA', 'EMA']},
                'direction': {'type': 'str', 'default': 'crossover', 'values': ['crossover', 'crossunder', 'above', 'below']},
                'equal': {'type': 'bool', 'default': False, 'description': 'Có xem xét giá trị bằng nhau không, ví dụ option src1 crossover src2, mà equal = True thì khi src1 = src2 cũng được xem là crossover, tương tự với crossunder, above, below'},
                "use_as_lookback_cond" : {'type': 'bool', 'default': False},
                'lookback_cond_nbar' : {'type': 'int', 'default': 5}
            }
        },
        'ursi': {
            'function': Conds.Indicators.ursi,
            'title': 'Ultimate RSI',
            'description': "Ultimate RSI",
            'inputs': ['src'],
            'params': {
                'length': {'type': 'int', 'default': 14},
                'smo_type1': {'type': 'str', 'default': 'RMA', 'values': ['SMA', 'EMA', 'RMA']},
                'smooth': {'type': 'int', 'default': 14},
                'smo_type2': {'type': 'str', 'default': 'EMA', 'values': ['SMA', 'EMA', 'RMA']},
                'direction': {'type': 'str', 'default': 'crossover', 'values': ['crossover', 'crossunder', 'above', 'below']},
                'equal': {'type': 'bool', 'default': False, 'description': 'Có xem xét giá trị bằng nhau không, ví dụ option src1 crossover src2, mà equal = True thì khi src1 = src2 cũng được xem là crossover, tương tự với crossunder, above, below'},
                'range_lower': {'type': 'float', 'default': 30},
                'range_upper': {'type': 'float', 'default': 70},
                'use_as_lookback_cond': {'type': 'bool', 'default': False},
                'lookback_cond_nbar': {'type': 'int', 'default': 5},
            }
        },
        'macd': {
            'function': Conds.Indicators.macd,
            'title': 'MACD',
            'description': "MACD",
            'inputs': ['src'],
            'params': {
                'r2_period': {'type': 'int', 'default': 20},
                'fast': {'type': 'int', 'default': 10},
                'slow': {'type': 'int', 'default': 20},
                'signal_length': {'type': 'int', 'default': 9},
                'direction': {'type': 'str', 'default': 'crossover', 'values': ['crossover', 'crossunder', 'above', 'below']},
                'equal': {'type': 'bool', 'default': False, 'description': 'Có xem xét giá trị bằng nhau không, ví dụ option src1 crossover src2, mà equal = True thì khi src1 = src2 cũng được xem là crossover, tương tự với crossunder, above, below'},
                'range_lower': {'type': 'float', 'default': 30},
                'range_upper': {'type': 'float', 'default': 70},
                'use_as_lookback_cond': {'type': 'bool', 'default': False},
                'lookback_cond_nbar': {'type': 'int', 'default': 5},
            }
        },
        'bbwp': {
            'function': Conds.Indicators.bbwp,
            'title': 'BBWP',
            'description': "Boillinger Band Width Percentile",
            'inputs': ['src'],
            'params': {
                'basic_type': {'type': 'str', 'default': 'SMA', 'values': ['SMA', 'EMA']},
                'bbwp_len': {'type': 'int', 'default': 13},
                'bbwp_lkbk': {'type': 'int', 'default': 128},
                'use_low_thres': {'type': 'bool', 'default': False},
                'low_thres': {'type': 'float', 'default': 20},
                'use_high_thres': {'type': 'bool', 'default': False},
                'high_thres': {'type': 'float', 'default': 80},
                'use_as_lookback_cond': {'type': 'bool', 'default': False},
                'lookback_cond_nbar': {'type': 'int', 'default': 5},
            }
        },
        'bbpctb': {
            'function': Conds.Indicators.bbpctb,
            'title': 'BB%B',
            'description': "Boillinger Band %B",
            'inputs': ['src'],
            'params': {
                'length': {'type': 'int', 'default': 20},
                'mult': {'type': 'float', 'default': 2},
                'direction': {'type': 'str', 'default': 'crossover', 'values': ['crossover', 'crossunder', 'above', 'below']},
                'cross_line': {'type': 'str', 'default': "Upper band", 'values': ["Upper band", "Lower band"]},
                'use_as_lookback_cond': {'type': 'bool', 'default': False},
                'lookback_cond_nbar': {'type': 'int', 'default': 5},
            }
        },
        'compare_two_sources': {
            'function': Conds.compare_two_sources,
            'title': 'Compare Two Sources',
            'description': "So sánh tỉ số (%) giữa nguồn dữ liệu src1 với src2 có nằm trong một khoảng xác định hay không",
            'inputs': ['src1', 'src2'],
            'params': {
                'lower_thres': {'type': 'float', 'default': -999},
                'upper_thres': {'type': 'float', 'default': 999},
            }
        }
    }

def input_source_functions():
    return {
        'MA': {
            'function': Ta.MA,
            'title': 'Moving Average',
            'description': "Moving average (EMA, SMA, RMA)",
            'inputs': ['src'],
            'params': {
                'window': {'type': 'int', 'default': 10},
                'ma_type': {'type': 'str', 'default': 'SMA', 'values': ['SMA', 'EMA', 'RMA']},
            },
            'outputs': {
                'output': {'type': 'line', 'mapping': 'MA', 'value': 'MA', 'color': Utils.random_color()}
            }
        },
        'ursi': {
            'function': Indicators.ursi,
            'title': 'Ultimate RSI',
            'description': "Ultimate Relative Strength Index",
            'inputs': ['src'],
            'params': {
                'length': {'type': 'int', 'default': 14},
                'smo_type1': {'type': 'str', 'default': 'RMA', 'values': ['SMA', 'EMA', 'RMA']},
                'smooth': {'type': 'int', 'default': 14},
                'smo_type2': {'type': 'str', 'default': 'EMA', 'values': ['SMA', 'EMA', 'RMA']},
            },
            'outputs': {
                'output1':{'type': 'line', 'mapping': 'arsi',        'value': 'ursi',        'color': Utils.random_color()},
                'output2':{'type': 'line', 'mapping': 'arsi_signal', 'value': 'ursi_signal', 'color': Utils.random_color()},
            }
        }, 
        'squeeze': {
            'function': Indicators.squeeze,
            'title': 'Squeeze',
            'description': "Squeeze indicator",
            'inputs': ['src', 'high', 'low', 'close'],
            'params': {
                'bb_length': {'type': 'int', 'default': 20},
                'length_kc': {'type': 'int', 'default': 20},
                'mult_kc': {'type': 'float', 'default': 1.5},
                'use_true_range': {'type': 'bool', 'default': False},
            },
            'outputs': {
                'output1':{'type': 'bool', 'mapping':'sqz_on',  'value': 'sqz_on',  'color': Utils.random_color()},
                'output2':{'type': 'bool', 'mapping':'sqz_off', 'value': 'sqz_off', 'color': Utils.random_color()},
            }
        },
        'macd': {
            'function': Indicators.macd,
            'title': 'MACD',
            'description': "Moving Average Convergence Divergence",
            'inputs': ['src'],
            'params': {
                'r2_period': {'type': 'int', 'default': 20},
                'fast': {'type': 'int', 'default': 10},
                'slow': {'type': 'int', 'default': 20},
                'signal_length': {'type': 'int', 'default': 9}
            },
            'outputs': {
                'output1': {'type': 'line', 'mapping': 'macd', 'value': 'macd', 'color': Utils.random_color()},
                'output2': {'type': 'line', 'mapping': 'macd_signal', 'value': 'macd_signal', 'color': Utils.random_color()}
            }
        },
        'bbwp': {
            'function': Indicators.bbwp,
            'title': 'Bollinger Bands Width Percentile',
            'description': "Bollinger Bands Width Percentile",
            'inputs': ['src'],
            'params': {
                'basic_type': {'type': 'str', 'default': 'SMA', 'values': ['SMA', 'EMA']},
                'bbwp_len': {'type': 'int', 'default': 13},
                'bbwp_lkbk': {'type': 'int', 'default': 128}
            },
            'outputs': {
                'output': {'type': 'line', 'mapping': 'bbwp', 'value': 'bbwp', 'color': Utils.random_color()}
            }
        },
        'bbpctb': {
            'function': Indicators.bbpctb,
            'title': 'Bollinger Bands %B',
            'description': "Bollinger Bands %B",
            'inputs': ['src'],
            'params': {
                'length': {'type': 'int', 'default': 20},
                'mult': {'type': 'float', 'default': 2}
            },
            'outputs': {
                'output1': {'type': 'line', 'mapping': 'BBpctB', 'value': 'BBpctB', 'color': Utils.random_color()},
                'output2': {'type': 'line', 'mapping': 'BBpctB_upper', 'value': 'BBpctB_upper', 'color': Utils.random_color()},
                'output3': {'type': 'line', 'mapping': 'BBpctB_lower', 'value': 'BBpctB_lower', 'color': Utils.random_color()}
            }
        },
        'subtract': {
            'function': Indicators.subtract,
            'title': 'Subtract Two Sources',
            'description': "Calculate the difference between two sources (src1 - src2)",
            'inputs': ['src1', 'src2'],
            'params': {},
            'outputs': {
                'output': {'type': 'line', 'mapping': 'net', 'value': 'net', 'color': Utils.random_color()}
            }
        },
        'percentile': {
            'function': Indicators.percentile,
            'title': 'Percentile Rank',
            'description': "Calculate rolling percentile rank of values",
            'inputs': ['src'],
            'params': {
                'window': {'type': 'int', 'default': 20}
            },
            'outputs': {
                'output': {'type': 'line', 'mapping': 'percentile', 'value': 'percentile', 'color': Utils.random_color()}
            }
        }
    }

class Resampler:
    
    @staticmethod
    def _calculate_candle_time(timestamps: pd.Series, timeframe):
        """
        Tính toán candleTime mới dựa trên timestamps và timeframe
        
        Parameters:
        -----------
        timestamps : int
            Timestamp gốc (nanoseconds)
        timeframe : str
            Timeframe mới ('1min', '5min', '10min', '30min', '1H', '2H', 'D')
        """

        # Chuyển timestamps từ nanoseconds sang seconds
        ts_seconds = timestamps // 1_000_000_000 

        tf_minutes = {
            '1min': 1,
            '5min': 5,
            '10min': 10,
            '30min': 30,
            '1H': 60,
            '2H': 120,
            'D': 1440
        }.get(timeframe) * 60

        base_candles = (ts_seconds // tf_minutes) * tf_minutes
        if timeframe == 'D':
            new_candles = base_candles + 9*60*60 + 15*60
        else:
            seconds_in_day = ts_seconds % 86400
            
            # Thời điểm ATC (14:45:00)
            atc_seconds = 14 * 3600 + 45 * 60  # = 53100
            
            new_candles = pd.Series(
                np.where(seconds_in_day == atc_seconds, ts_seconds, base_candles),
                index = timestamps.index
            )

        return new_candles * 1_000_000_000

    @staticmethod
    def _validate_agg_dict(df: pd.DataFrame, agg_dict: dict):
        """
        Kiểm tra và chuẩn hóa agg_dict
        """
        if not agg_dict:
            return {col: 'last' for col in df.columns if col != 'datetime'}
        
        # Kiểm tra columns không tồn tại
        invalid_cols = set(agg_dict.keys()) - set(df.columns)
        if invalid_cols:
            raise ValueError(f"Columns không tồn tại trong DataFrame: {invalid_cols}")
        
        # Thêm columns còn thiếu với aggregation mặc định là 'last'
        missing_cols = set(df.columns) - set(agg_dict.keys()) - {'datetime'}
        if missing_cols:
            for col in missing_cols:
                agg_dict[col] = 'last'
        
        return agg_dict

    @classmethod
    def resample_vn_stock_data(cls, df: pd.DataFrame, timeframe='1min', agg_dict=None):
        """
        Resample data chứng khoán theo timeframe với custom aggregation
        
        Parameters:
        -----------
        df : pandas.DataFrame
            DataFrame gốc với index là candleTime (nanoseconds)
        timeframe : str
            Timeframe mới ('1min', '5min', '10min', '30min', '1H', '2H', 'D')
        agg_dict : dict
            Dictionary chỉ định phương thức aggregation cho từng column
        """

        # def test():
        #     df: pd.DataFrame = sample_data.copy()
        #     timeframe = '30min'
        #     agg_dict = {'buyImpact': 'last', 'sellImpact': 'last'}

        # Validate và chuẩn hóa agg_dict
        agg_dict = cls._validate_agg_dict(df, agg_dict)
        
        # Reset index để có thể xử lý candleTime
        df = df.reset_index()

        # Tính candleTime mới cho mỗi row
        df['candleTime'] = cls._calculate_candle_time(
                timestamps=df['candleTime'],
                timeframe=timeframe
            )

        df['test'] = pd.to_datetime(df.index)
        df['test2'] = pd.to_datetime(df['candleTime'])
        
        # Group theo candleTime mới và áp dụng aggregation
        grouped = df.groupby('candleTime').agg(agg_dict)
        
        return grouped.sort_index()


class Ta:
    """Technical analysis with vectorized operations for both Series and DataFrame"""
    
    @staticmethod
    def make_lookback(cond: pd.Series, lookback_period):
        if lookback_period == 0:
            return cond

        return cond.rolling(lookback_period, closed = 'left').max().fillna(False).astype(bool)

    @staticmethod
    def rolling_rank(src: PandasObject, ranking_window):
        """Calculate rolling rank percentile for each value
        
        Args:
            src: pd.Series or pd.DataFrame
            ranking_window: int, window size for rolling calculation
            
        Returns:
            Same type as input with rolling rank values scaled to 0-100
        """
        return (src.rolling(ranking_window).rank() - 1) / (ranking_window - 1) * 100

    @staticmethod
    def crossover(src1: PandasObject, src2: PandasObject):
        """Check if src1 crosses over src2 using vectorized operations
        
        Args:
            src1: pd.Series or pd.DataFrame 
            src2: pd.Series or pd.DataFrame
            
        Returns:
            Boolean Series/DataFrame where True indicates crossover points
        """
        # Current bar condition: src1 >= src2
        curr_cond = src1 >= src2
        
        # Previous bar condition: src1 < src2 
        prev_cond = src1.shift(1) < src2.shift(1)
        
        return curr_cond & prev_cond

    @staticmethod
    def crossunder(src1: PandasObject, src2: PandasObject):
        """Check if src1 crosses under src2 using vectorized operations
        
        Args:
            src1: pd.Series or pd.DataFrame
            src2: pd.Series or pd.DataFrame
            
        Returns:
            Boolean Series/DataFrame where True indicates crossunder points
        """
        # Current bar condition: src1 <= src2 
        curr_cond = src1 <= src2
        
        # Previous bar condition: src1 > src2
        prev_cond = src1.shift(1) > src2.shift(1)
        
        return curr_cond & prev_cond
        
    @staticmethod 
    def streak_count(condition: PandasObject):
        """Count consecutive occurrences of a condition
        
        Args:
            condition: Boolean Series/DataFrame
                
        Returns:
            Same type as input with count of consecutive True values, resets on False
        """
        if isinstance(condition, pd.DataFrame):
            # Convert to numpy array for faster computation
            arr = condition.astype(int).values
            
            # Create array to accumulate counts
            result = np.zeros_like(arr)
            
            # Set first row based on condition
            result[0] = arr[0]
            
            # Accumulate counts where True, reset where False
            for i in range(1, len(arr)):
                result[i] = (result[i-1] + 1) * arr[i]
                
            # Convert back to DataFrame with original index and columns
            return pd.DataFrame(result, index=condition.index, columns=condition.columns)
        else:
            # Original logic for Series
            groups = (~condition).cumsum()
            return condition.groupby(groups).cumsum() * condition
        
    @staticmethod
    def highest(src: PandasObject, window):
        """Rolling highest value
        
        Args:
            src: pd.Series or pd.DataFrame
            window: int, lookback period
            
        Returns:
            Rolling maximum values
        """
        return src.rolling(window=window).max()
        
    @staticmethod
    def lowest(src: PandasObject, window):
        """Rolling lowest value
        
        Args:
            src: pd.Series or pd.DataFrame
            window: int, lookback period
            
        Returns:
            Rolling minimum values
        """
        return src.rolling(window=window).min()

    @staticmethod 
    def sma(src: PandasObject, window: int):
        """Simple moving average
        
        Args:
            src: pd.Series or pd.DataFrame
            window: int, moving average period
            
        Returns:
            Simple moving average values
        """
        return src.rolling(window=window).mean()

    @staticmethod
    def ema(src: PandasObject, window: int):
        """Exponential moving average
        
        Args:
            src: pd.Series or pd.DataFrame
            window: int, moving average period
            
        Returns:
            Exponential moving average values
        """
        return src.ewm(span=window, adjust=False).mean()
    
    @staticmethod
    def rma(src: PandasObject, length: int):
        """Moving average used in RSI.
        It is the exponentially weighted moving average with alpha = 1 / length.

        Args:
            src (PandasObject): Series of values to process.
            length (_type_): Number of bars (length).

        Returns:
            Exponential moving average of source with alpha = 1 / length.
        """
        return src.ewm(alpha=1 / length, adjust=False).mean()
    
    @staticmethod
    def MA(src: PandasObject, window, ma_type='SMA'):
        """Moving average calculation
        
        Args:
            src: pd.Series or pd.DataFrame
            window: int, moving average period
            ma_type: str, 'SMA' or 'EMA'
            
        Returns:
            Moving average values
        """

        if isinstance(src, pd.Series):
            src.name = "MA"

        if ma_type == 'SMA':
            return Ta.sma(src, window)
        elif ma_type == 'EMA':
            return Ta.ema(src, window)
        elif ma_type == 'RMA':
            return Ta.rma(src, window)
        else:
            raise ValueError("Invalid moving average type. Choose one in ['RMA', 'SMA', 'EMA']")

    @staticmethod
    def roc(src: PandasObject, window):
        """Rate of change in percentage
        
        Args:
            src: pd.Series or pd.DataFrame
            window: int, lookback period
            
        Returns: 
            Percentage change over window period
        """
        return (src / src.shift(window) - 1) * 100

    @staticmethod
    def rsi(src: PandasObject, window):
        """Relative Strength Index
        
        Args:
            src: pd.Series or pd.DataFrame
            window: int, RSI period
            
        Returns:
            RSI values ranging from 0 to 100
        """
        # Calculate price changes
        delta = src.diff()
        
        # Separate positive and negative changes
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        
        # Calculate average gain and loss
        avg_gain = gain.rolling(window=window).mean()
        avg_loss = loss.rolling(window=window).mean()
        
        # Calculate RS and RSI
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    @staticmethod
    def momentum(src: PandasObject, window):
        """Momentum indicator
        
        Args:
            src: pd.Series or pd.DataFrame
            window: int, lookback period
            
        Returns:
            Momentum values
        """
        return src - src.shift(window)

    @staticmethod
    def std_dev(src: PandasObject, window):
        """Rolling standard deviation
        
        Args:
            src: pd.Series or pd.DataFrame
            window: int, lookback period
            
        Returns:
            Rolling standard deviation values
        """
        return src.rolling(window=window).std(ddof=0)

    @staticmethod
    def z_score(src: PandasObject, window):
        """Calculate Z-score based on rolling mean and std
        
        Args:
            src: pd.Series or pd.DataFrame
            window: int, lookback period
            
        Returns:
            Z-score values
        """
        rolling_mean = src.rolling(window=window).mean()
        rolling_std = src.rolling(window=window).std()
        return (src - rolling_mean) / rolling_std
    
    @staticmethod
    def max_obj(df_ls: list[PandasObject]) -> PandasObject:
        """Calculate maximum value between multiple pandas objects (Series or DataFrames)
        
        Args:
            df_ls (List[PandasObject]): List of Series or DataFrames to compare
            
        Returns:
            PandasObject: Maximum values in same format as input
        """
        # Check if inputs are Series
        if all(isinstance(x, pd.Series) for x in df_ls):
            return pd.Series(np.maximum.reduce([x.values for x in df_ls]), 
                           index=df_ls[0].index)
        
        # If DataFrames
        return pd.DataFrame(
            np.maximum.reduce([df.to_numpy() for df in df_ls]),
            columns=df_ls[0].columns, 
            index=df_ls[0].index
        )
    
    @staticmethod
    def apply_rolling(data: PandasObject, window: int, method: str = 'sum'):
        """Apply rolling operation based on specified method
        
        Args:
            data: Input series/dataframe
            window: Rolling window size
            method: Rolling method - 'sum', 'median', 'mean', 'last', 'rank'
            
        Returns:
            Rolled data with specified method
        """
        rolling = data.rolling(window)
        
        if method == 'sum':
            return rolling.sum()
        elif method == 'median':
            return rolling.median() 
        elif method == 'mean':
            return rolling.mean()
        elif method == 'rank':
            return (rolling.rank() - 1) / (window - 1) * 100
        else:
            raise ValueError(f"Invalid rolling method: {method}. Must be one of: sum, median, mean, rank")


class Indicators:
    """Technical indicators with vectorized operations for both Series and DataFrame"""
    @staticmethod
    def two_MA(src: PandasObject, ma1: int, ma2: int, ma_type: str = 'SMA'):
        """Calculate two moving averages
        
        Args:
            src: pd.Series or pd.DataFrame
            ma1: int, period for first moving average
            ma2: int, period for second moving average
            
        Returns:
            tuple: (ma1, ma2) Series or DataFrames depending on input
        """
        return Ta.MA(src, ma1, ma_type), Ta.MA(src, ma2, ma_type)

    @staticmethod
    def squeeze(
        src: PandasObject,
        high: PandasObject,
        low: PandasObject,
        close: PandasObject,
        bb_length: int = 20,
        length_kc: int = 20,
        mult_kc: float = 1.5,
        use_true_range: bool = True

    ):
        """Calculate squeeze indicator for both regular and multi-index DataFrames
        
        Args:
            df (pd.DataFrame): OHLCV DataFrame (regular or multi-index columns)
            src_name (str): Column name for source values
            bb_length (int): Bollinger Bands length
            length_kc (int): Keltner Channel length
            mult_kc (float): Keltner Channel multiplier
            use_true_range (bool): Use true range for KC calculation
            
        Returns:
            tuple: (sqz_on, sqz_off, no_sqz) Series or DataFrames depending on input
        """
        def test():
            src = Adapters.load_index_ohlcv_from_plasma('F1')['close']
            high = Adapters.load_index_ohlcv_from_plasma('F1')['high']
            low = Adapters.load_index_ohlcv_from_plasma('F1')['low']
            close = Adapters.load_index_ohlcv_from_plasma('F1')['close']
            bb_length = 20
            length_kc = 20
            mult_kc = 1.5
            use_true_range = True

        # For regular DataFrame, get Series
        p_h = high
        p_l = low
        p_c = close

        # Calculate BB
        basic = Ta.sma(src, bb_length)
        dev = mult_kc * Ta.std_dev(src, bb_length)
        upper_bb = basic + dev
        lower_bb = basic - dev

        # Calculate KC
        sqz_ma = Ta.sma(src, length_kc)
        
        # Calculate range
        if use_true_range:
            sqz_range = Ta.max_obj([
                p_h - p_l,
                (p_h - p_c.shift(1)).abs(),
                (p_l - p_c.shift(1)).abs()
            ])
        else:
            sqz_range = p_h - p_l

        rangema = Ta.sma(sqz_range, length_kc)
        upper_kc = sqz_ma + rangema * mult_kc
        lower_kc = sqz_ma - rangema * mult_kc

        # Calculate squeeze conditions
        sqz_on = (lower_bb > lower_kc) & (upper_bb < upper_kc)
        sqz_off = (lower_bb < lower_kc) & (upper_bb > upper_kc)
        # no_sqz = ~(sqz_on | sqz_off)

        if isinstance(src, pd.Series):
            sqz_on.name = 'sqz_on'
            sqz_off.name = 'sqz_off'

        return sqz_on, sqz_off
    
    @staticmethod
    def ursi(
        src: PandasObject,
        length: int = 14,
        smo_type1: str = "RMA",
        smooth: int = 14,
        smo_type2: str = "EMA"
    ):
        """Ultimate RSI

        Args:
            src (pd.Series or pd.DataFrame): Input source of the indicator (open, high, low, close)

            length (int, optional): Calculation period of the indicator. Defaults to 14.

            smo_type1 (str, optional): Smoothing method used for the calculation of the indicator.
            Defaults to 'RMA'.

            smooth (int, optional): Degree of smoothness of the signal line. Defaults to 14.

            smo_type2 (str, optional): Smoothing method used to calculation the signal line.
            Defaults to 'EMA'.

        Returns:
            arsi (pd.Series or pd.DataFrame): ursi line
            signal (pd.Series or pd.DataFrame): ursi's signal line
        """
        def test():
            src = Adapters.load_index_daily_ohlcv_from_plasma()['F1Close']
            length: int = 14
            smo_type1: str = "SMA"
            smooth: int = 14
            smo_type2: str = "EMA"

        upper = Ta.highest(src, length)
        lower = Ta.lowest(src, length)
        r = upper - lower
        d = src.diff()

        diff = np.where(upper > upper.shift(1), r, np.where(lower < lower.shift(1), -r, d))
        diff = pd.DataFrame(diff, index=src.index)

        num = Ta.MA(diff, length, smo_type1)
        den = Ta.MA(diff.abs(), length, smo_type1)
        arsi = (num / den) * 50 + 50
        signal = Ta.MA(arsi, smooth, smo_type2)

        if isinstance(src, pd.Series):
            arsi = arsi[0]
            signal = signal[0]
            arsi.name = 'arsi'
            signal.name = 'arsi_signal'
        
        return arsi, signal

    @staticmethod
    def macd(
        src: PandasObject,
        r2_period: int = 20,
        fast: int = 10,
        slow: int = 20,
        signal_length: int = 9,
    ):
        """Calculate MACD"""

        def test():
            src = Adapters.load_stock_data_from_plasma()['close']
            r2_period: int = 20
            fast: int = 10
            slow: int = 20
            signal_length: int = 9

        origin_index = src.index

        src = src.reset_index(drop=True).copy()
        bar_index = range(len(src))

        a1 = 2 / (fast + 1)
        a2 = 2 / (slow + 1)

        correlation = src.rolling(r2_period).corr(pd.Series(bar_index))
        r2 = 0.5 * correlation**2 + 0.5
        K = r2 * ((1 - a1) * (1 - a2)) + (1 - r2) * ((1 - a1) / (1 - a2))

        var1 = src.diff().fillna(0) * (a1 - a2)
        var1 = var1.to_numpy()
        K = K.to_numpy()

        i = 2
        np_macd =  np.zeros_like(src) * np.nan
        prev = np.nan_to_num(np_macd[i - 1])
        prev_prev = np.nan_to_num(np_macd[i - 2])
        for i in range(2, len(src)):
            current = np_macd[i] = (
                var1[i]
                + (-a2 - a1 + 2) * np.nan_to_num(prev)
                - K[i] * np.nan_to_num(prev_prev)
            )
            prev_prev = prev
            prev = current
        
        if isinstance(src, pd.Series):
            macd = pd.Series(np_macd, index=origin_index)
            macd.name = 'macd'
            
        else:
            macd = pd.DataFrame(np_macd, index=origin_index, columns=src.columns)

        signal = Ta.ema(macd, signal_length)
        if isinstance(macd, pd.Series):
            signal.name = 'macd_signal'

        return macd, signal

    @staticmethod
    def bbwp(
        src: PandasObject,
        basic_type: str = "SMA",
        bbwp_len: int = 13,
        bbwp_lkbk: int = 128,
    ):
        def test():
            src = Adapters.load_stock_data_from_plasma()['close']
            basic_type: str = "SMA"
            bbwp_len: int = 13
            bbwp_lkbk: int = 128

        """bbwp"""
        _price = src

        _basic = Ta.MA(_price, bbwp_len, basic_type)
        _dev = Ta.std_dev(_price, bbwp_len)
        _bbw = (_basic + _dev - (_basic - _dev)) / _basic

        index = src.reset_index(drop=True).index
        bbwp_denominator = pd.Series(np.where(index < bbwp_lkbk, index, bbwp_lkbk), index=src.index)
        _bbw_sum = (
            _bbw.rolling(
                bbwp_lkbk + 1, 
                min_periods= bbwp_len
                ).rank() - 1
            ).div(bbwp_denominator, axis=0) * 100

        if isinstance(src, pd.Series):
            _bbw_sum.name = 'bbwp'

        return _bbw_sum
    
    @staticmethod
    def bbpctb(
        src: PandasObject, 
        length: int = 20, 
        mult: float = 2, 
    ):
        """bbpctb"""

        def test():
            src = Adapters.load_stock_data_from_plasma()['close']
            length: int = 20
            mult: float = 2 
            return_upper_lower = False

        basic = Ta.sma(src, length)
        dev = mult * Ta.std_dev(src, length)
        upper = basic + dev
        lower = basic - dev
        bbpctb = (src - lower) / (upper - lower) * 100

        if isinstance(src, pd.Series):
            bbpctb.name = 'BBpctB'
            upper.name = 'BBpctB_upper'
            lower.name = 'BBpctB_lower'

        return bbpctb, upper, lower

    @staticmethod
    def subtract(src1: PandasObject, src2: PandasObject):
        """Calculate the difference between two sources"""
        result: PandasObject = src1 - src2
        
        if isinstance(src1, pd.Series):
            result.name = 'net'
            
        return result

    @staticmethod
    def percentile(src: PandasObject, window: int = 20):
        """Calculate rolling percentile rank"""
        result: PandasObject = Ta.rolling_rank(src, window)
        
        if isinstance(src, pd.Series):
            result.name = 'percentile'
            
        return result

   

class Conds:
    """Market condition analysis functions with vectorized operations"""
    @staticmethod
    def min_inc_dec_bars(
        src: PandasObject,
        n_bars: int = 10,
        n_bars_inc: int = None,
        n_bars_dec: int = None,
        use_as_lookback_cond: bool = False,
        lookback_cond_nbar = 5,
        useflag: bool = True
    ):
        """Count increasing and decreasing bars in a rolling window
        
        Args:
            src: pd.Series or pd.DataFrame
            n_bars: Length of rolling window
            n_bars_inc: Minimum number of increasing bars required
            n_bars_dec: Minimum number of decreasing bars required
        """

        if not useflag:
            return None
            
        if n_bars_inc is None and n_bars_dec is None:
            raise ValueError("At least one of n_bars_inc or n_bars_dec must be specified")
        
        if n_bars_inc is not None and n_bars_inc > n_bars:
            raise ValueError("n_bars_inc cannot be greater than n_bars")
        
        if n_bars_dec is not None and n_bars_dec > n_bars:
            raise ValueError("n_bars_dec cannot be greater than n_bars")
        
        # Calculate changes
        changes = src.diff()
        
        # Create masks for increases and decreases
        inc_mask = (changes > 0).astype(int)
        dec_mask = (changes < 0).astype(int)
        
        # Calculate rolling sums
        inc_count = inc_mask.rolling(n_bars).sum()
        dec_count = dec_mask.rolling(n_bars).sum()
        
        # Apply conditions
        inc_condition = True if n_bars_inc is None else inc_count >= n_bars_inc
        dec_condition = True if n_bars_dec is None else dec_count >= n_bars_dec
        
        # Combine conditions
        result: PandasObject = inc_condition & dec_condition
        
        # Set initial values to False
        if isinstance(result, pd.DataFrame):
            result.iloc[:n_bars-1, :] = False
        else:
            result.iloc[:n_bars-1] = False
            
        if use_as_lookback_cond:
            result = Ta.make_lookback(result, lookback_cond_nbar)

        return result

    @staticmethod
    def two_line_pos(
        src1: PandasObject,
        src2: PandasObject,
        direction: str = 'crossover',
        equal: bool = False,
        use_as_lookback_cond: bool = False,
        lookback_cond_nbar = 5,
        useflag: bool = True
    ):
        """Check relative position of two lines
        
        Args:
            src1: pd.Series or pd.DataFrame 
            src2: pd.Series or pd.DataFrame
            direction: 'crossover', 'crossunder', 'above', or 'below'
            equal: Whether to include equal values in comparison
        """

        if not useflag:
            return None
            
        if direction == "crossover":
            result = Ta.crossover(src1, src2)
        elif direction == "crossunder":
            result = Ta.crossunder(src1, src2)
        elif direction == "above":
            result = src1 >= src2 if equal else src1 > src2
        elif direction == "below":
            result = src1 <= src2 if equal else src1 < src2
        
        if use_as_lookback_cond:
            result = Ta.make_lookback(result, lookback_cond_nbar)

        return result

    @staticmethod
    def consecutive_above_below(
        src1: PandasObject,
        src2: PandasObject,
        direction: str = 'above',
        num_bars: int = 5,
        use_as_lookback_cond: bool = False,
        lookback_cond_nbar = 5,
        useflag: bool = True
    ):
        """Check for consecutive bars where one line is above/below another
        
        Args:
            src1: pd.Series or pd.DataFrame
            src2: pd.Series or pd.DataFrame
            direction: 'above' or 'below'
            num_bars: Required number of consecutive bars
        """
        if not useflag:
            return None
        
        # Create condition based on direction
        condition = src1 > src2 if direction == "above" else src1 < src2
        
        # Count consecutive occurrences
        streak = Ta.streak_count(condition)
        
        result = streak >= num_bars
    
        if use_as_lookback_cond:
            result = Ta.make_lookback(result, lookback_cond_nbar)
        return result

    @staticmethod
    def cross_no_reverse(
        src1: PandasObject,
        src2: PandasObject,
        direction: str = "crossover",
        bars_no_reverse: int = 5,
        use_as_lookback_cond: bool = False,
        lookback_cond_nbar = 5,
        useflag: bool = True
    ):
        """Check for crosses without reversal within specified period
        
        Args:
            src1: pd.Series or pd.DataFrame
            src2: pd.Series or pd.DataFrame
            direction: 'crossover' or 'crossunder'
            bars_no_reverse: Number of bars to check for no reversal
        """
        if not useflag:
            return None

        # Get initial cross signals
        cross_signals = Ta.crossover(src1, src2) if direction == "crossover" else Ta.crossunder(src1, src2)
        reverse_signals = Ta.crossunder(src1, src2) if direction == "crossover" else Ta.crossover(src1, src2)

        # Check for reverse signals in forward window
        reverse_windows = reverse_signals.rolling(window=bars_no_reverse, min_periods=1).sum()

        # Valid signals: cross points with no reverse signals in next X bars
        valid_signals = cross_signals.shift(bars_no_reverse) & (reverse_windows == 0)
        result = valid_signals.fillna(False)
    
        if use_as_lookback_cond:
            result = Ta.make_lookback(result, lookback_cond_nbar)
        return result

    @staticmethod
    def is_in_top_bot_percentile(
        src: PandasObject,
        lookback_period: int = 20,
        direction: str = 'top',
        threshold: float = 90,
        use_as_lookback_cond: bool = False,
        lookback_cond_nbar = 5,
        useflag: bool = True
    ):
        """Check if values are in top/bottom percentile range
        
        Args:
            src: pd.Series or pd.DataFrame
            lookback_period: Period for percentile calculation
            direction: 'top' or 'bottom'
            threshold: Percentile threshold
        """
        if not useflag:
            return None
        
        rank = Ta.rolling_rank(src, lookback_period)
        thres = threshold if direction == 'top' else 100 - threshold
        result =  rank >= thres if direction == 'top' else rank <= thres
    
        if use_as_lookback_cond:
            result = Ta.make_lookback(result, lookback_cond_nbar)
        return result

    @staticmethod
    def range_nbars(
        src: PandasObject,
        lower_thres: float,
        upper_thres: float,
        sum_nbars: int = 1,
        use_as_lookback_cond: bool = False,
        lookback_cond_nbar = 5,
        useflag: bool = True
    ):
        """Check if values are within specified range
        
        Args:
            line: pd.Series or pd.DataFrame
            lower_thres: Lower bound of range
            upper_thres: Upper bound of range
        """
        if not useflag:
            return None
        
        src = src.rolling(sum_nbars).sum()
        
        result = (src >= lower_thres) & (src <= upper_thres)
    
        if use_as_lookback_cond:
            result = Ta.make_lookback(result, lookback_cond_nbar)
        return result

    @staticmethod
    def range_cond(
        src: PandasObject,
        lower_thres: float,
        upper_thres: float,
        use_as_lookback_cond: bool = False,
        lookback_cond_nbar = 5,
        useflag: bool = True
    ):
        """Check if values are within specified range
        
        Args:
            src: pd.Series or pd.DataFrame
            lower_thres: Lower bound of range
            upper_thres: Upper bound of range
        """
        if not useflag:
            return None
        
        result = (src >= lower_thres) & (src <= upper_thres)
    
        if use_as_lookback_cond:
            result = Ta.make_lookback(result, lookback_cond_nbar)
        return result

    @staticmethod
    def percent_change_in_range(
        src: PandasObject,
        n_bars: int = 1,
        lower_thres: float = 0,
        upper_thres: float = 100,
        use_as_lookback_cond: bool = False,
        lookback_cond_nbar = 5,
        useflag: bool = True
    ):
        """Check if percent change is within specified range
        
        Args:
            src: pd.Series or pd.DataFrame
            n_bars: Period for calculating change
            lower_thres: Lower bound for percent change
            upper_thres: Upper bound for percent change
        """
        if not useflag:
            return None
        
        pct_change = src.pct_change(n_bars) * 100
        result = (pct_change >= lower_thres) & (pct_change <= upper_thres)
    
        if use_as_lookback_cond:
            result = Ta.make_lookback(result, lookback_cond_nbar)
        return result

    @staticmethod
    def absolute_change_in_range(
        src: PandasObject,
        n_bars: int = 1,
        lower_thres: float = -999,
        upper_thres: float = 999,
        use_as_lookback_cond: bool = False,
        lookback_cond_nbar = 5,
        useflag: bool = True
    ):
        """Check if absolute change is within specified range
        
        Args:
            src: pd.Series or pd.DataFrame
            n_bars: Period for calculating change
            lower_thres: Lower bound for absolute change
            upper_thres: Upper bound for absolute change
        """

        if not useflag:
            return None
        

        change = src.diff(periods=n_bars)
        result = (change >= lower_thres) & (change <= upper_thres)
    
        if use_as_lookback_cond:
            result = Ta.make_lookback(result, lookback_cond_nbar)
        return result

    @staticmethod
    def gap_trend(
        src1: PandasObject,
        src2: PandasObject,
        sign: str = "positive",
        trend_direction: str = "increase",
        trend_bars: int = 5,
        use_as_lookback_cond: bool = False,
        lookback_cond_nbar = 5,
        useflag: bool = True
    ):
        """Analyze trend in gap between two lines
        
        Args:
            src1: pd.Series or pd.DataFrame
            src2: pd.Series or pd.DataFrame
            sign: 'positive' or 'negative' gap
            trend_direction: 'increase' or 'decrease'
            trend_bars: Number of bars for trend confirmation
        """
        if not useflag:
            return None
        
        # Calculate gap and its properties
        gap = src1 - src2
        gap_sign_cond = gap > 0 if sign == "positive" else gap < 0
        gap_change = gap.diff()
        trend_cond = gap_change > 0 if trend_direction == "increase" else gap_change < 0
            
        # Check if condition holds for specified number of bars
        result = (trend_cond & gap_sign_cond).rolling(window=trend_bars).sum() >= trend_bars
    
        if use_as_lookback_cond:
            result = Ta.make_lookback(result, lookback_cond_nbar)
        return result

    @staticmethod
    def gap_percentile(
        src1: PandasObject,
        src2: PandasObject,
        lookback_period: int = 20,
        threshold: float = 90,
        use_as_lookback_cond: bool = False,
        lookback_cond_nbar = 5,
        useflag: bool = True
    ):
        """Check if gap between lines is in high percentile
        
        Args:
            src1: pd.Series or pd.DataFrame
            src2: pd.Series or pd.DataFrame
            lookback_period: Period for percentile calculation
            threshold: Percentile threshold
        """
        if not useflag:
            return None
        
        gap = abs(src1 - src2)
        gap_rank = Ta.rolling_rank(gap, lookback_period)
        result = gap_rank >= threshold
    
        if use_as_lookback_cond:
            result = Ta.make_lookback(result, lookback_cond_nbar)
        return result

    @staticmethod
    def gap_range(
        src1: PandasObject,
        src2: PandasObject,
        lower_thres: float = -999,
        upper_thres: float = 999,
        use_as_lookback_cond: bool = False,
        lookback_cond_nbar = 5,
    ):
        """Check if gap between lines is within specified range
        
        Args:
            src1: pd.Series or pd.DataFrame
            src2: pd.Series or pd.DataFrame
            lower_thres: Lower bound for gap
            upper_thres: Upper bound for gap
        """
        gap = src1 - src2
        result = (gap >= lower_thres) & (gap <= upper_thres)
        if use_as_lookback_cond:
            result = Ta.make_lookback(result, lookback_cond_nbar)
        return result

    @staticmethod
    def percentile_in_range(
        src: PandasObject,
        lookback_period: int = 20,
        lower_thres: float = 10,
        upper_thres: float = 90,
        use_as_lookback_cond: bool = False,
        lookback_cond_nbar = 5,
        ):
        """Check if values are within specified percentile range"""
        rank = Ta.rolling_rank(src, lookback_period)
        result = (rank >= lower_thres) & (rank <= upper_thres)
        if use_as_lookback_cond:
            result = Ta.make_lookback(result, lookback_cond_nbar)
        return result

    @staticmethod
    def compare_two_sources(src1: PandasObject, src2: PandasObject, lower_thres: float, upper_thres: float):
        """Compare two sources with threshold"""
        def test():
            src1 = Adapters.load_group_data_from_plasma(['fBuyVal'], groups=['All'])['fBuyVal']
            src2 = Adapters.load_group_data_from_plasma(['fSellVal'], groups=['All'])['fSellVal']
            lower_thres = 700
            upper_thres = 1000

        change = src1  / src2 * 100

        result = (change >= lower_thres) & (change <= upper_thres)
        return result
        
    class Indicators:
        @staticmethod
        def consecutive_squeezes(
            src: PandasObject,
            high: PandasObject,
            low: PandasObject,
            close: PandasObject,
            bb_length: int = 20,
            length_kc: int = 20,
            mult_kc: float = 1.5,
            num_bars_sqz: int = 5,
            use_no_sqz: bool = False,
            use_as_lookback_cond: bool = False,
            lookback_cond_nbar = 5,
        ):
            bb_length = int(bb_length)
            length_kc = int(length_kc)
            num_bars_sqz = int(num_bars_sqz)

            sqz_on, sqz_off  = Indicators.squeeze(
                src, high, low, close, bb_length, length_kc, mult_kc
            )

            if use_no_sqz:
                result = sqz_off
            else:
                cons_sqz_num = Ta.streak_count(sqz_on)
                result = cons_sqz_num >= num_bars_sqz

            if use_as_lookback_cond:
                result = Ta.make_lookback(result, lookback_cond_nbar)
            return result

        @staticmethod
        def ursi(
            src: PandasObject,
            length: int = 14,
            smo_type1: str = "RMA",
            smooth: int = 14,
            smo_type2: str = "EMA",
            direction: str = 'crossover',
            equal: bool = False,
            range_lower: float = 30,
            range_upper: float = 70,
            use_as_lookback_cond: bool = False,
            lookback_cond_nbar = 5,
        ):
            arsi, signal = Indicators.ursi(src, length, smo_type1, smooth, smo_type2)

            pos_cond = Conds.two_line_pos(arsi, signal, direction, equal)
            range_cond = Conds.range_cond(arsi, range_lower, range_upper)

            result = pos_cond & range_cond

            if use_as_lookback_cond:
                result = Ta.make_lookback(result, lookback_cond_nbar)

            return result
        
        @staticmethod
        def macd(
            src: PandasObject,
            r2_period: int = 20,
            fast: int = 10,
            slow: int = 20,
            signal_length: int = 9,
            direction: str = 'crossover',
            equal: bool = False,
            range_lower: float = 30,
            range_upper: float = 70,
            use_as_lookback_cond: bool = False,
            lookback_cond_nbar = 5,
        ):
            macd, signal = Indicators.macd(src, r2_period, fast, slow, signal_length)

            pos_cond = Conds.two_line_pos(macd, signal, direction, equal)
            range_cond = Conds.range_cond(macd, range_lower, range_upper)

            result = pos_cond & range_cond

            if use_as_lookback_cond:
                result = Ta.make_lookback(result, lookback_cond_nbar)

            return result

        @staticmethod
        def bbwp(
            src: PandasObject,
            basic_type: str = "SMA",
            bbwp_len: int = 13,
            bbwp_lkbk: int = 128,
            use_low_thres: bool = False,
            low_thres: float = 20,
            use_high_thres: bool = False,
            high_thres: float = 80,
            use_as_lookback_cond: bool = False,
            lookback_cond_nbar = 5,
        ):
            bbwp = Indicators.bbwp(src, basic_type, bbwp_len, bbwp_lkbk)

            result = None

            if use_low_thres:
                result = bbwp <= low_thres

            if use_high_thres:
                result = bbwp >= high_thres

            if use_as_lookback_cond and result is not None:
                result = Ta.make_lookback(result, lookback_cond_nbar)

            return result

        @staticmethod
        def bbpctb(
            src: PandasObject,
            length: int = 20,
            mult: float = 2,
            direction: str = "crossover",
            cross_line: str = "Upper band",
            use_as_lookback_cond: bool = False,
            lookback_cond_nbar = 5,
        ):
            """bbpctb based conditions"""

            def test():
                src = Adapters.load_index_daily_ohlcv_from_plasma()['F1Close']
                length: int = 20
                mult: float = 2 
                return_upper_lower = False
                direction: str = "crossover"
                cross_line: str = "Upper band"

            length = int(length)

            bbpctb, _, _ = Indicators.bbpctb(src, length, mult)

            cross_l = Utils.new_1val_pdObj(100 if cross_line == "Upper band" else 0, src)

            res = Conds.two_line_pos(
                src1=bbpctb,
                src2=cross_l,
                direction=direction
            )

            if use_as_lookback_cond:
                res = Ta.make_lookback(res, lookback_cond_nbar)

            return res
    
        @staticmethod
        def two_MA_pos(
            src: PandasObject,
            ma1: int,
            ma2: int,
            ma_type: str = 'SMA',
            direction: str = 'crossover',
            equal: bool = False,
            use_as_lookback_cond: bool = False,
            lookback_cond_nbar = 5,
        ):
            ma1_line, ma2_line = Indicators.two_MA(src, ma1, ma2, ma_type)
            return Conds.two_line_pos(ma1_line, ma2_line, direction, equal, use_as_lookback_cond, lookback_cond_nbar)


class CombiConds:
    @staticmethod
    def load_and_process_group_data(conditions_params: list[dict], use_sample_data = False):

        def test():
            conditions_params = [
                {
                    'function': "is_in_top_bot_percentile",
                    'inputs': {
                        'src': 'bu2',
                        'stocks': ['VN30', 'Super High Beta'],
                        'rolling_timeframe': '15Min'
                    },
                    'params': {
                        'lookback_period': 1000,
                        'direction': 'top',
                        'threshold': 90,
                        'use_as_lookback_cond': False,
                        'lookback_cond_nbar': 5
                    }
                },
                {
                    'function': 'gap_trend',
                    'inputs': {
                        'src1': 'bid',
                        'src2': 'ask',
                        'stocks': ['HPG', 'SSI', 'NVL']
                    },
                    'params': {
                        'direction': 'above'
                    }
                },
                {
                    'function': 'two_line_pos',
                    'inputs': {
                        'src1': 'bu',
                        'src2': 'sd',
                        'stocks': ['SSI', 'NVL', "VN30"],
                    },
                    'params': {
                        'direction': 'crossover'
                    }
                },
            ]

        required_data = {}
        original_cols = set()
        rolling_cols = set()
        
        updated_conditions = []
        for condition in conditions_params:
            # Ưu tiên rolling_window nếu có, nếu không thì dùng rolling_timeframe
            rolling_window = condition['inputs'].get('rolling_window')
            rolling_tf = condition['inputs'].get('rolling_timeframe')
            rolling_method = condition['inputs'].get('rolling_method', 'sum')
            stocks = condition['inputs'].get('stocks', Globs.STOCKS)
            stocks_key = hash('_'.join(sorted(stocks)))
            
            new_condition = deepcopy(condition)
            new_condition['inputs'] = {}
            
            for param_name, col_name in condition['inputs'].items():
                if param_name not in ['rolling_timeframe', 'rolling_window', 'stocks', 'rolling_method']:
                    if col_name == "":
                        raise InputSourceEmptyError(f"Input {param_name} is empty!")
                    if rolling_window is not None or rolling_tf:
                        rolling_tf_val = rolling_window if rolling_window is not None else Utils.convert_timeframe_to_rolling(rolling_tf)
                        rolling_cols.add((col_name, rolling_tf_val, stocks_key, rolling_method))
                        new_key = f"{col_name}_{rolling_tf_val}_{stocks_key}_{rolling_method}"
                    else:
                        original_cols.add((col_name, stocks_key))
                        new_key = f"{col_name}_None_{stocks_key}"
                    new_condition['inputs'][param_name] = new_key
                else:
                    new_condition['inputs'][param_name] = condition['inputs'][param_name]
            
            updated_conditions.append(new_condition)

        # Group all required columns by unique stocks combinations
        all_cols_by_stocks = {}
        for col, stocks_key in original_cols:
            if stocks_key not in all_cols_by_stocks:
                all_cols_by_stocks[stocks_key] = set()
            all_cols_by_stocks[stocks_key].add(col)
            
        for col, tf, stocks_key, _ in rolling_cols:
            if stocks_key not in all_cols_by_stocks:
                all_cols_by_stocks[stocks_key] = set()
            all_cols_by_stocks[stocks_key].add(col)

        # Load data for each unique stocks combination  
        for stocks_key, cols in all_cols_by_stocks.items():
            filtered_stocks = Globs.STOCKS
            for condition in conditions_params:
                stocks = condition['inputs'].get('stocks', Globs.STOCKS)
                if hash('_'.join(sorted(stocks))) == stocks_key:
                    filtered_stocks = stocks
                    break

            data = Adapters.load_groups_and_stocks_data_from_plasma(list(cols), filtered_stocks, use_sample_data)
            data = data.groupby(level=0, axis=1).sum()
            
            # Add original columns
            for col, sk in original_cols:
                if sk == stocks_key:
                    key = f"{col}_None_{stocks_key}"
                    required_data[key] = data[col]
            
            # Add rolled data with specified method
            for col, tf, sk, method in rolling_cols:
                if sk == stocks_key:
                    key = f"{col}_{tf}_{sk}_{method}"
                    required_data[key] = Ta.apply_rolling(data[col], tf, method)

        return required_data, updated_conditions

    @staticmethod
    def load_and_process_one_series_data(conditions_params: list[dict], data_src: str = 'market_stats', use_sample_data=False):

        def test():
            conditions_params = [
                {
                    "function": "absolute_change_in_range",
                    "inputs": {
                    "src": "VnindexClose",
                    },
                    "params": {
                    "n_bars": 5,
                    "lower_thres": 1,
                    "upper_thres": 999,
                    "use_as_lookback_cond": False,
                    "lookback_cond_nbar": 5
                    }
                }
            ]

        assert data_src in ['market_stats', 'daily_index'], "`data_src` must be one in `['market_stats', 'daily_index']"

        required_data = {}
        original_cols = set()
        rolling_cols = set()
        
        updated_conditions = []
        for condition in conditions_params:
            # Ưu tiên rolling_window nếu có, nếu không thì dùng rolling_timeframe
            rolling_window = condition['inputs'].get('rolling_window')
            rolling_tf = condition['inputs'].get('rolling_timeframe') 
            rolling_method = condition['inputs'].get('rolling_method', 'sum')
            
            new_condition = deepcopy(condition)
            new_condition['inputs'] = {}

            for param_name, col_name in condition['inputs'].items():
                if param_name not in ['rolling_timeframe', 'rolling_window', 'stocks', 'rolling_method']:
                    if col_name == "":
                        raise InputSourceEmptyError(f"Input {param_name} is empty!")
                    
                    if rolling_window is not None or rolling_tf:
                        rolling_tf_val = rolling_window if rolling_window is not None else Utils.convert_timeframe_to_rolling(rolling_tf)
                        rolling_cols.add((col_name, rolling_tf_val, rolling_method))
                        new_key = f"{col_name}_{rolling_tf_val}_{rolling_method}"
                    else:
                        original_cols.add(col_name)
                        new_key = f"{col_name}_None"
                    new_condition['inputs'][param_name] = new_key
                else:
                    new_condition['inputs'][param_name] = condition['inputs'][param_name]
            
            updated_conditions.append(new_condition)
        
        all_cols = {col for col in original_cols} | {col for col, _, _ in rolling_cols}

        if data_src == 'market_stats':
            data = Adapters.load_market_stats_from_plasma(list(all_cols), use_sample_data)
        else:
            data = Adapters.load_index_daily_ohlcv_from_plasma(list(all_cols), use_sample_data)
        
        # Add original columns
        for col in original_cols:
            key = f"{col}_None"
            required_data[key] = data[col]
        
        # Add rolled data with specified method
        for col, tf, method in rolling_cols:
            key = f"{col}_{tf}_{method}"
            required_data[key] = Ta.apply_rolling(data[col], tf, method)

        return required_data, updated_conditions

    @staticmethod
    def load_and_process_stock_data(conditions_params: list[dict], stocks: list = None, use_sample_data=False):
        def test():
            conditions_params = [
                {
                    "function": "absolute_change_in_range",
                    "inputs": {
                    "src": "bu",
                    "rolling_timeframe": "15Min",
                    "rolling_method": "sum"
                    },
                    "params": {
                    "n_bars": 1,
                    "lower_thres": 10,
                    "upper_thres": 999,
                    "use_as_lookback_cond": False,
                    "lookback_cond_nbar": 5
                    }
                }
            ]
            stocks = None
            use_sample_data = True

        required_data = {}
        original_cols = set()
        rolling_cols = set()
        
        updated_conditions = []
        for condition in conditions_params:
            # Ưu tiên rolling_window nếu có, nếu không thì dùng rolling_timeframe
            rolling_window = condition['inputs'].get('rolling_window')
            rolling_tf = condition['inputs'].get('rolling_timeframe')
            rolling_method = condition['inputs'].get('rolling_method', 'sum')
            
            new_condition = deepcopy(condition)
            new_condition['inputs'] = {}
            
            for param_name, col_name in condition['inputs'].items():
                if param_name not in ['rolling_timeframe', 'rolling_window', 'stocks', 'rolling_method']:
                    if col_name == "":
                        raise InputSourceEmptyError(f"Input {param_name} is empty!")

                    if rolling_window is not None or rolling_tf:
                        rolling_tf_val = rolling_window if rolling_window is not None else Utils.convert_timeframe_to_rolling(rolling_tf)
                        rolling_cols.add((col_name, rolling_tf_val, rolling_method))
                        new_key = f"{col_name}_{rolling_tf_val}_{rolling_method}"
                    else:
                        original_cols.add(col_name)
                        new_key = f"{col_name}_None"
                    new_condition['inputs'][param_name] = new_key
                else:
                    new_condition['inputs'][param_name] = condition['inputs'][param_name]
            
            updated_conditions.append(new_condition)
        
        all_cols = {col for col in original_cols} | {col for col, _, _ in rolling_cols}

        data = Adapters.load_stock_data_from_plasma(list(all_cols), stocks=stocks, load_sample=use_sample_data)
        
        # Add original columns
        for col in original_cols:
            key = f"{col}_None"
            required_data[key] = data[col]
        
        # Add rolled data with specified method
        for col, tf, method in rolling_cols:
            key = f"{col}_{tf}_{method}"
            required_data[key] = Ta.apply_rolling(data[col], tf, method)

        return required_data, updated_conditions
    
    @staticmethod
    def combine_conditions(required_data: dict[str, pd.DataFrame], conditions_params: list[dict]):
        """Combine multiple conditions with optimized data loading and rolling operations"""
        # Get function mapping
        func_map = function_mapping()
        
        # Process conditions and combine results
        result = None
        for condition in conditions_params:
            # Skip if useflag is False
            if not condition['params'].get('useflag', True):
                continue
                
            # Get condition function
            func = func_map[condition['function']]['function']
            
            # Prepare input data - now just use the pre-calculated keys directly
            func_inputs = {
                param_name: required_data[col_name]
                for param_name, col_name in condition['inputs'].items()
                if param_name not in ['rolling_timeframe', 'rolling_method', 'rolling_window', 'stocks']
            }

            # Apply condition function
            signal = func(**func_inputs, **condition['params'])

            # Combine with final result using AND operation
            result = Utils.and_conditions([result, signal])
        
        return result


class QuerryData:

    @staticmethod
    def load_data(required_data: dict[str, pd.DataFrame], conditions_params: list[dict], sub_rediskey: str=''):
        def test():
            other_params = [
                {
                    "function": "MA",
                    "inputs": {
                        "src": "Arbit",
                    },
                    "params": {
                        'window': 10,
                        'ma_type': 'EMA'
                    },
                    'outputs': {
                        'output': {'type': 'line', 'mapping': 'MA', 'value': 'MA', 'color': Utils.random_color()}
                    }
                },
                {
                    "function": "MA",
                    "inputs": {
                        "src": "Arbit",
                    },
                    "params": {
                        'window': 15,
                        'ma_type': 'EMA'
                    },
                    'outputs': {
                        'output': {'type': 'line', 'mapping': 'MA', 'value': 'MA1', 'color': Utils.random_color()}
                    }
                },
                {
                    "function": "ursi",
                    "inputs": {
                        "src": "F1Close",
                    },
                    "params": {
                        'length': 14,
                        'smo_type1': 'RMA',
                        'smooth': 14,
                        'smo_type2': 'EMA'
                    },
                    'outputs': {
                        'output1':{'type': 'line', 'mapping': 'arsi', 'value': 'ursi', 'color': Utils.random_color()},
                        'output2':{'type': 'line', 'mapping': 'arsi_signal', 'value': 'ursi_signal', 'color': Utils.random_color()},
                    }
                }
            ]

            dailyindex_params = [
                {
                    "function": "MA",
                    "inputs": {
                        "src": "F1Close",
                    },
                    "params": {
                        'window': 10,
                        'ma_type': 'EMA'
                    },
                    'outputs': {
                        'output': {'type': 'line', 'mapping': 'MA', 'value': 'MA3', 'color': Utils.random_color()}
                    }
                }
            ]

            required_data, conditions_params  = CombiConds.load_and_process_one_series_data(
                conditions_params=dailyindex_params, data_src='daily_index'
            )

        redis_handler = RedisHandler()

        # Get function mapping
        func_map = input_source_functions()
        
        # Process conditions and combine results
        result = None
        data_info = []
        for condition in conditions_params:
                
            # Get condition function
            func = func_map[condition['function']]['function']
            
            # Prepare input data - now just use the pre-calculated keys directly
            func_inputs = {
                param_name: required_data[col_name]
                for param_name, col_name in condition['inputs'].items()
                if param_name not in ['rolling_timeframe', 'rolling_method', 'rolling_window', 'stocks']
            }

            def _redis_cache(func_inputs, condition):
                hash_condition = {k: v for k, v in condition.items() if k != 'outputs'}

                key = redis_handler.create_hash_key(str(hash_condition), prefix=f'pslab/stockcount/source/{sub_rediskey}')

                func_outputs = {
                    k['mapping']: k['value'] for k in condition['outputs'].values()
                }

                style_mapping = {
                    k['value'] : k['type'] for k in condition['outputs'].values()
                }

                color_mapping = {
                    k['value'] : k['color'] for k in condition['outputs'].values()
                }

                col_mapping = {
                    k['value'] : k['mapping'] for k in condition['outputs'].values()
                }

                if redis_handler.check_exist(key):
                    df_data = redis_handler.get_key(key, pickle_data=True)
                else:
                    data = func(**func_inputs, **condition['params'])
                    if isinstance(data, pd.Series):
                        df_data = pd.DataFrame(data)
                    else:
                        df_data = pd.concat(data, axis=1)
                    redis_handler.set_key_with_ttl(key, df_data, pickle_data=True)

                df_data: pd.DataFrame = df_data.rename(columns=func_outputs)

                info = [{'name': c, 'key': f"{key}-{col_mapping[c]}", 'type': style_mapping[c], 'color': color_mapping[c], 'mapping':col_mapping[c]} for c in df_data.columns]

                return df_data, info

            df_data, info = _redis_cache(func_inputs, condition)

            data_info.extend(info)

            if result is None:
                result = df_data.copy()
            else:
                result = pd.concat([result, df_data], axis=1)

        return result, data_info

    def load_all_data(group_params, other_params, dailyindex_params, start_day, end_day):

        def test():
            group_params = [
                {
                    "function": "MA",
                    "inputs": {
                        "src": "bu2",
                        "stocks": ["All"]
                    },
                    "params": {
                        'window': 10,
                        'ma_type': 'EMA'
                    },
                    'outputs': {
                        'output': {'type': 'line', 'mapping': 'MA', 'value': 'MA', 'color': '#92c12e'}
                    }
                }
            ]

            other_params = [
                {
                    "function": "MA",
                    "inputs": {
                        "src": "Arbit",
                    },
                    "params": {
                        'window': 30,
                        'ma_type': 'EMA'
                    },
                    'outputs': {
                        'output': {'type': 'line', 'mapping': 'MA', 'value': 'MA2', 'color': '#0c8fbd'}
                    }
                }
            ]

            dailyindex_params = [
                {
                    "function": "MA",
                    "inputs": {
                        "src": "F1Close",
                    },
                    "params": {
                        'window': 10,
                        'ma_type': 'EMA'
                    },
                    'outputs': {
                        'output': {'type': 'line', 'mapping': 'MA', 'value': 'MA3', 'color': '#c027a1'}
                    }
                }
            ]

            start_day = '2025_01_13'
            end_day = '2025_01_14'

        start_timestamp = Utils.day_to_timestamp(start_day)
        end_timestamp = Utils.day_to_timestamp(end_day, is_end_day=True)

        def _process_group_data(group_params):
            if not group_params:
                return None, None
            required_data, updated_params = CombiConds.load_and_process_group_data(group_params)
            return QuerryData.load_data(required_data, updated_params)
        
        def _process_other_data(other_params):
            if not other_params:
                return None, None
            required_data, updated_params = CombiConds.load_and_process_one_series_data(other_params)
            return QuerryData.load_data(required_data, updated_params)
        
        def _process_indexdaily_data(dailyindex_params):
            if not dailyindex_params:
                return None, None
            required_data, updated_params = CombiConds.load_and_process_one_series_data(dailyindex_params, data_src='daily_index')
            return QuerryData.load_data(required_data, updated_params, sub_rediskey='dailyindex')
        
        result_df = None
        result_info = []

        group_data, group_info = _process_group_data(group_params)
        if group_data is not None:
            result_df = group_data
            result_info.extend(group_info)

        other_data, other_info = _process_other_data(other_params)
        if other_data is not None:
            result_df = pd.concat([result_df, other_data], axis=1) if result_df is not None else other_data
            result_info.extend(other_info)

        def _daily_to_timestamp(df: pd.DataFrame):
            df['datetime'] = pd.to_datetime(df.index + ' ' + '09:15:00', format = '%Y_%m_%d %H:%M:%S')
            df['candleTime'] = df['datetime'].astype(int)
            df = df.set_index('candleTime')
            df = df.drop('datetime', axis=1)
            return df
        
        indexdaily_data, indexdaily_info = _process_indexdaily_data(dailyindex_params)
        if indexdaily_data is not None:
            if result_df is None:
                df_ohlc = Adapters.load_index_ohlcv_from_plasma(name="VNINDEX")
  
                def _convert_daily_to_intraday(df: pd.DataFrame, intraday_index: pd.Series):
                    def test():
                        df = indexdaily_data
                        intraday_index = df_ohlc.index

                    df = _daily_to_timestamp(df)

                    intraday_index = pd.DataFrame(intraday_index)

                    intraday_index = intraday_index.set_index('candleTime')
                    intraday_index = intraday_index.sort_values('candleTime')

                    df = pd.concat([intraday_index, df], axis=1, join='outer')    
                    df = df.fillna(method='ffill')
                    
                    return df

                result_df = _convert_daily_to_intraday(indexdaily_data, df_ohlc.index)
            else:
                indexdaily_data = _daily_to_timestamp(indexdaily_data)

                result_df = pd.concat([result_df, indexdaily_data], axis=1) if result_df is not None else indexdaily_data 
                result_df = result_df.fillna(method='ffill')
                
                result_info.extend(indexdaily_info)


        if result_df is None:
            return {}, {}
        
        result_df = result_df[(result_df.index >= start_timestamp) & (result_df.index <= end_timestamp)]


        res = result_df.reset_index().to_dict('records')
        res = remove_nan_keys(res)

        return res, result_info


@dataclass
class ReturnStatsConfig:
    """Configuration for return statistics calculations"""
    lookback_periods: int = 5
    use_pct: bool = False  # If True, calculate percentage returns

class ReturnStats:
    """Class for calculating trading returns and statistics from signals"""
    
    @staticmethod
    def calculate_returns(
        df_ohlc: pd.DataFrame,
        signals: pd.Series,
        config: ReturnStatsConfig
    ) -> pd.DataFrame:
        """
        Calculate various return metrics for trading signals.
        
        Args:
            df_ohlc: DataFrame with OHLC data and 'day' column
            signals: Boolean Series with trading signals
            config: Configuration parameters
            
        Returns:
            DataFrame with calculated returns and metrics
        """
        # Ensure index alignment
        df = df_ohlc.copy()
        df['matched'] = signals
        df['matched_nan'] = np.where(df['matched'].notna(), 1, np.NaN)
        
        # Calculate entry price (next bar's open)
        df['enter_price'] = df['open'].shift(-1)
        
        # Calculate returns
        df['c_o'] = ReturnStats._compute_return(
            df['open'].shift(-(config.lookback_periods+1)),
            df['enter_price'],
            config.use_pct
        )
        
        df['h_o'] = ReturnStats._compute_return(
            df['high'].rolling(config.lookback_periods, closed='right').max().shift(-config.lookback_periods),
            df['enter_price'],
            config.use_pct
        )
        
        df['l_o'] = ReturnStats._compute_return(
            df['low'].rolling(config.lookback_periods, closed='right').min().shift(-config.lookback_periods),
            df['enter_price'],
            config.use_pct
        )
        
        # Calculate matched returns
        df['matched_c_o'] = df['matched_nan'] * df['c_o']
        df['matched_h_o'] = df['matched_nan'] * df['h_o']
        df['matched_l_o'] = df['matched_nan'] * df['l_o']
        
        # Calculate win/loss metrics
        df['wintrade'] = np.where(df['matched_c_o'] > 0, 1, 0)
        df['losstrade'] = np.where(df['matched_c_o'] <= 0, 1, 0)
        df['return_fill'] = df['matched_nan'] * df['c_o']
        
        return df[df['matched'] == True]

    @staticmethod
    def _compute_return(a: Union[float, pd.Series], b: Union[float, pd.Series], use_pct: bool = False) -> Union[float, pd.Series]:
        """Calculate return between two values"""
        if use_pct:
            return (a / b - 1) * 100
        return a - b
            
    @staticmethod
    def get_statistics(df: pd.DataFrame) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Generate comprehensive statistics for the trading results.
        
        Args:
            df: DataFrame with calculated returns
            
        Returns:
            Dictionary containing yearly, monthly, and daily statistics
        """
        # Extract year and month info
        df['year'] = df['day'].str[:4]
        df['yearmonth'] = df['day'].str[:7]
        
        return {
            'yearly': ReturnStats._calculate_yearly_stats(df),
            'monthly': ReturnStats._calculate_monthly_stats(df),
            'daily': ReturnStats._calculate_daily_stats(df)
        }

    @staticmethod
    def _calculate_yearly_stats(df: pd.DataFrame) -> pd.DataFrame:
        """Calculate yearly trading statistics"""
        stats = df.groupby('year')['matched'].count().to_frame('numtrade')
        
        # Calculate average returns
        for col in ['c_o', 'h_o', 'l_o']:
            matched_col = f'matched_{col}'
            stats[f'avg_{col}'] = (
                df.groupby('year')[matched_col].sum() / stats['numtrade']
            )
        
        # Calculate win rate
        stats['winrate'] = (
            df.groupby('year')['wintrade'].sum() / stats['numtrade'] * 100
        )
        
        numtrade = df['matched_c_o'].count()

        # Add total statistics
        total_stats = pd.Series({
            'numtrade': numtrade,
            'avg_c_o': df['matched_c_o'].mean(),
            'avg_h_o': df['matched_h_o'].mean(),
            'avg_l_o': df['matched_l_o'].mean(),
            'winrate': (df['wintrade'].sum() / numtrade * 100)
            if numtrade > 0 else 0
        }, name='Total')
        
        return pd.concat([stats, total_stats.to_frame().T])

    @staticmethod
    def _calculate_monthly_stats(df: pd.DataFrame) -> pd.DataFrame:
        """Calculate monthly trading statistics"""
        stats = df.groupby('yearmonth')['matched_c_o'].count().to_frame('numtrade')
        
        # Calculate average returns
        for col in ['c_o', 'h_o', 'l_o']:
            matched_col = f'matched_{col}'
            stats[f'avg_{col}'] = (
                df.groupby('yearmonth')[matched_col].sum() / stats['numtrade']
            )
        
        # Calculate win rate
        stats['winrate'] = (
            df.groupby('yearmonth')['wintrade'].sum() / stats['numtrade'] * 100
        )
        
        return stats

    @staticmethod
    def _calculate_daily_stats(df: pd.DataFrame) -> pd.DataFrame:
        """Calculate daily cumulative returns"""
        daily_returns = df.groupby('day')['return_fill'].sum()
        return daily_returns.cumsum().to_frame('cum_return')

    @staticmethod
    def get_trade_summary(df: pd.DataFrame) -> Dict[str, float]:
        """
        Generate a summary of trading performance metrics.
        
        Args:
            df: DataFrame with calculated returns
            
        Returns:
            Dictionary containing key performance metrics including number of entry days
        """
        signal_rows = df[df['matched'] == True]
        non_nan_returns = signal_rows['c_o'].dropna()
        
        if len(non_nan_returns) > 0:
            # Calculate number of unique trading days
            num_entry_days = signal_rows['day'].nunique()
            
            return {
                'Number of Trades': len(non_nan_returns),
                'Number Entry Days': num_entry_days,  # Added this metric
                'Win Rate (%)': (non_nan_returns > 0).mean() * 100,
                'Average Return': non_nan_returns.mean(),
                'Average High Return': signal_rows['h_o'].dropna().mean(),
                'Average Low Return': signal_rows['l_o'].dropna().mean(),
                'Max Return': non_nan_returns.max(),
                'Min Return': non_nan_returns.min(),
                'Profit Factor': abs(non_nan_returns[non_nan_returns > 0].sum() / 
                                non_nan_returns[non_nan_returns < 0].sum()) 
                if len(non_nan_returns[non_nan_returns < 0]) > 0 else float('inf'),
                'Average Win': non_nan_returns[non_nan_returns > 0].mean()
                if len(non_nan_returns[non_nan_returns > 0]) > 0 else 0,
                'Average Loss': non_nan_returns[non_nan_returns <= 0].mean()
                if len(non_nan_returns[non_nan_returns <= 0]) > 0 else 0
            }
        else:
            return {
                'Number of Trades': 0,
                'Number Entry Days': 0,  # Added this metric
                'Win Rate (%)': 0,
                'Average Return': 0,
                'Average High Return': 0,
                'Average Low Return': 0,
                'Max Return': 0,
                'Min Return': 0,
                'Profit Factor': 0,
                'Average Win': 0,
                'Average Loss': 0
            }


# {
#     'sector': ['VN30', 'SOBank'],
#     'beta': ['Low', 'Medium'],
#     'marketCapIndex': ['VN30', 'VNSML']
# }

# # 'gap_trend(src1=bid,src2=ask,stocks=VN30,rolling_timeframe=30Min,sign=negative,trend_direction=increase,trend_bars=10,use_as_lookback_cond=True,lookback_cond_nbar=5) | 
# # range_nbars(line=Unwind,lower_thres=20,upper_thres=80,sum_nbars=1,use_as_lookback_cond=True,lookback_cond_nbar=10)'
# conditions_params = [
#   {
#     "function": "absolute_change_in_range",
#     "inputs": {
#       "src": [
#                 {
#                     "function": "absolute_change_in_range",
#                     "inputs": {
#                     "src": "bu",
#                     "rolling_timeframe": "15Min"
#                     },
#                     "params": {
#                     "n_bars": 1,
#                     "lower_thres": 10,
#                     "upper_thres": 999,
#                     "use_as_lookback_cond": False,
#                     "lookback_cond_nbar": 5
#                     }
#                 }
#             ]
#       "rolling_timeframe": "15Min"
#     },
#     "params": {
#       "n_bars": 1,
#       "lower_thres": 10,
#       "upper_thres": 999,
#       "use_as_lookback_cond": False,
#       "lookback_cond_nbar": 5
#     }
#   }
# ]


# ls = [{
#         "function": "absolute_change_in_range",
#         "inputs": {
#         "src": "bu",
#         "rolling_timeframe": "15Min"
#         },
#         "params": {
#         "n_bars": 1,
#         "lower_thres": 10,
#         "upper_thres": 999,
#         "use_as_lookback_cond": False,
#         "lookback_cond_nbar": 5
#         }
#     }]
# hash(str(ls))
# required_data, updated_params = CombiConds.load_and_process_stock_data(conditions_params)
# # # # Generate signals
# signals = CombiConds.combine_conditions(required_data, updated_params)

# # 'two_line_pos(src1=bu2,src2=sd2,stocks=VN30,rolling_timeframe=30Min,direction=crossover,use_as_lookback_cond=True,lookback_cond_nbar=5)
# # range_nbars(line=Unwind,lower_thres=20,upper_thres=80,sum_nbars=5,use_as_lookback_cond=False,lookback_cond_nbar=5)'

# # 'cross_no_reverse(src1=bu2,src2=sd2,stocks=Super High Beta,rolling_timeframe=15Min,direction=crossover,bars_no_reverse=10,use_as_lookback_cond=True,lookback_cond_nbar=5) 
# # range_nbars(line=Unwind,lower_thres=30,upper_thres=70,sum_nbars=10,use_as_lookback_cond=True,lookback_cond_nbar=5)'

# Conds.absolute_change_in_range(required_data['bu_30'], **conditions_params)