from fastapi import APIRouter, Depends, Body, HTTPException
from helper.Reply import Reply
from helper.Timer import *
from config.authen import *
from constants.Constant import *
from helper.Defaults import exceptionInfo
from helper.Authen import checkAuthen, reusable_oauth2
from typing import Optional
from helper.Data import getMapping

#%%
from danglib.pslab.resources import Adapters, Globs
from danglib.pslab.utils import day_to_timestamp, Utils, RedisHandler
from danglib.pslab.funcs import function_mapping, CombiConds, Ta, pd, ReturnStats, ReturnStatsConfig, np, input_source_functions, QuerryData
from redis import StrictRedis
import hashlib
from datetime import timedelta
import pickle
from typing import Any, Union, Dict, List

class NoRedisDataError(Exception):
    """Raised when required Redis data is not found"""
    pass

class InputSourceEmptyError(Exception):
    """Raise when Input sources are empty"""
    pass

class DuplicateValueError(Exception):
    """Custom exception for duplicate values in outputs"""
    def __init__(self, duplicate_values: List[str]):
        self.duplicate_values = duplicate_values
        super().__init__(f"Duplicate values found in outputs: {duplicate_values}")

def validate_unique_output_values(params_list: List[dict]) -> None:
    """
    Validates that all output 'value' fields are unique across all parameter groups
    
    Args:
        params_list: List of parameter dictionaries containing output configurations
        
    Raises:
        DuplicateValueError: If duplicate output values are found
    """
    seen_values = set()
    duplicates = []
    
    for params in params_list:
        if not params or 'outputs' not in params:
            continue
            
        outputs = params.get('outputs', {})
        for output_key, output_config in outputs.items():
            value = output_config.get('value')
            if value:
                if value in seen_values:
                    duplicates.append(value)
                seen_values.add(value)
    
    if duplicates:
        raise DuplicateValueError(duplicates)


redis_handler = RedisHandler()

USE_SAMPLE_DATA = Globs.USE_SAMPLE_DATA

class PSLabHelper:
    @staticmethod
    def clean_for_json(obj: Any) -> Union[Dict, List, float, bool, str, None]:
        """
        Clean data structures to ensure they are JSON serializable.
        Handles pandas DataFrames/Series, numpy types, NaN values, etc.
        
        Args:
            obj: Any Python object that needs to be converted to JSON serializable format
            
        Returns:
            JSON serializable version of the input object
            
        Examples:
            >>> clean_for_json(pd.Series([1, 2, np.nan]))
            {0: 1.0, 1: 2.0, 2: 0}
            
            >>> clean_for_json(np.int64(42))
            42.0
            
            >>> clean_for_json({'a': np.array([1, 2]), 'b': pd.NA})
            {'a': [1.0, 2.0], 'b': 0}
        """
        if isinstance(obj, dict):
            return {k: PSLabHelper.clean_for_json(v) for k, v in obj.items() if v is not None}
        elif isinstance(obj, (list, tuple)):
            return [PSLabHelper.clean_for_json(item) for item in obj]
        elif isinstance(obj, (pd.DataFrame, pd.Series)):
            return PSLabHelper.clean_for_json(obj.to_dict())
        elif isinstance(obj, (np.integer, np.floating)):
            return float(obj)
        elif isinstance(obj, np.bool_):
            return bool(obj)
        elif pd.isna(obj) or obj is pd.NA:
            return 0
        elif isinstance(obj, np.ndarray):
            return PSLabHelper.clean_for_json(obj.tolist())
        return obj

    @staticmethod
    def convert_function_mapping():
        """Convert the original function mapping to the desired format"""
        mapping = function_mapping()
        result = []
        
        for func_name, details in mapping.items():
            # Create new dict without the original function object
            converted = {
                'function': func_name,  # Replace function object with function name
                'title': details['title'],
                'description': details['description'],
                'inputs': details['inputs'],
                'params': details['params']
            }
            result.append(converted)
        
        return result
    
    @staticmethod
    def convert_input_function_mapping():
        """Convert the original function mapping to the desired format"""
        mapping = input_source_functions()
        result = []
        
        for func_name, details in mapping.items():
            # Create new dict without the original function object
            converted = {
                'function': func_name,  # Replace function object with function name
                'title': details['title'],
                'description': details['description'],
                'inputs': details['inputs'],
                'outputs': details['outputs'],
                'params': details['params']
            }
            result.append(converted)
        
        return result

    @staticmethod
    def stock_count(
        conditions_params: list[dict], 
        stocks: list = None,
        count_smoothlen: int = 5, 
        count_dups: bool = False, 
        cons_nbar: int = 10, 
        percentage_smoothlen: int = 5, 
        percentile_lookback_nbar: int = 256,
        line: str = 'count'
    ):  
        required_data, updated_params = CombiConds.load_and_process_stock_data(conditions_params, use_sample_data=USE_SAMPLE_DATA, stocks = stocks)

        def test():
            conditions_params = [
                {
                    "function": "consecutive_above_below",
                    "inputs": {
                        "line1": "bu",
                        "line2": "sd"
                    },
                    "params": {
                        "direction": "above",
                        "num_bars": 5
                    }
                }
            ]
            

            from danglib.pslab.funcs import Conds

            Conds.consecutive_above_below(
                line1=required_data['bu_None'], 
                line2=required_data['sd_None'],
                **updated_params[0]['params']
                )

        matched = CombiConds.combine_conditions(required_data, updated_params)
        if matched is None:
            df_count = Utils.new_1val_series(0, list(required_data.values())[0]).to_frame("count")
            return df_count
        else:
            assert line in ['count', 'consCount', 'percentage', 'percentile'], "`line` must be one of ['count', 'consCount', 'percentage', 'percentile']"
            
            df_count = matched.sum(axis=1)

            if line == 'count':
                return Ta.sma(df_count, count_smoothlen).to_frame('count')
            
            if line == 'consCount': 
                if count_dups:
                    consCount = df_count.rolling(cons_nbar).sum()
                else:
                    df_lookback = matched.rolling(cons_nbar).max()
                    consCount = df_lookback.sum(axis=1)
                return Ta.sma(consCount, count_smoothlen).to_frame('count')

            if line == 'percentage':
                num_stocks = len(matched.columns)
                percentage = (df_count / num_stocks) * 100
                return Ta.sma(percentage, percentage_smoothlen).to_frame('count')

            if line == 'percentile':
                return df_count.rolling(percentile_lookback_nbar).rank(pct=True).to_frame('count')

    @staticmethod
    def create_condition_hash_key(count_conditions: dict):

        def _hash_sha256(value):
            # Chuyển thành bytes nếu đầu vào là chuỗi
            value_bytes = value.encode('utf-8') if isinstance(value, str) else value
            # Tính hash bằng SHA-256
            hash_object = hashlib.sha256(value_bytes)
            return hash_object.hexdigest()  # Trả về dạng chuỗi hex

        def test():
            count_conditions = [
                {
                    'function': 'two_line_pos',
                    'inputs': {
                        'line1': 'bu',
                        'line2': 'sd',
                        'rolling_timeframe': '15Min'
                    },
                    'params': {
                        'direction': 'crossover'
                    }
                },
                {
                    'function': "is_in_top_bot_percentile",
                    'inputs': {
                        'src': 'bu2',
                        'rolling_timeframe': '15Min'
                    },
                    'params': {
                        'lookback_period': 1000,
                        'direction': 'top',
                        'threshold': 90,
                        'use_as_lookback_cond': True,
                        'lookback_cond_nbar': 5
                    }
                }
            ]

        hashed_count_conditions = f"pslab/stockcount/{_hash_sha256(str(count_conditions))}"
        return hashed_count_conditions

    @staticmethod
    def get_dic_info():
        # general_funcs = [
        #     'absolute_change_in_range', 
        #     'consecutive_above_below', 
        #     'cross_no_reverse', 
        #     'gap_percentile', 
        #     'gap_trend', 
        #     'is_in_top_bot_percentile', 
        #     'min_inc_dec_bars', 
        #     'percent_change_in_range', 
        #     'range_nbars', 
        #     'range_cond', 
        #     'two_line_pos'
        # ]

        # ohlcv_funcs = ['consecutive_squeeze']
        general_funcs = [
            "Absolute Change in Range", 
            "Consecutive Above/Below",
            "Cross Without Reversal",
            "Gap Percentile",
            "Gap Trend Analysis",
            "Gap Range",
            "Top/Bottom Percentile",
            "Minimum Increasing/Decreasing Bars",
            "Percent Change in Range",
            "Percentile in Range",
            "Total value over n bars in Range",
            "Value in Range",
            "Two Line Position",
            "Two MA Line Position",
            "Compare Two Sources"
        ]

        ohlcv_funcs = ["Consecutive Squeezes", "Ultimate RSI", "MACD", "BBWP", "BB%B"]


        tooltips = {
            "Enter Condition": "Chọn điều kiện vào ra lệnh",
            "Add line": "Thêm đường stock count mới",
            "Select Chart": "Chọn INDEX để hiển thị: F1 (Mặc định), VN30, VNINDEX",
            "Select Conditions": "Chọn điều kiện để đếm số lượng cổ phiếu thỏa mãn",
            "Count Line": "Chọn điều kiện ra vào lệnh dựa trên đường stock count",
            "Group": "Chọn điều kiện ra vào lệnh dựa vào các chỉ số của các nhóm cổ phiếu",
            "Other": "Chọn điều kiện ra vào lệnh dựa vào các chỉ số khác của thị trường",
            "Index": "Chọn điều kiện ra vào lệnh dựa vào OHLCV của INDEX (VNINDEX, VN30...)",
            "Look-ahead periods": "Số lượng nến chờ đóng lệnh (Mặc định: 60)",
            "Index": "Chọn chỉ số để vào ra lệnh (F1, VN30...)",
            "Use TimeFrame": """Chọn rolling timeframe tính tổng cho nến 30s 
                            (Ví dụ: chọn 15Min thì sẽ rolling 30 sum nến 30s, 5Min là 10 nến 30s, ...), 
                            nếu chọn Timeframe sẽ rolling sum tất cả các inputs"""
        }

        beta_list = ['Super High Beta', 'High Beta', 'Medium', 'Low']
        sectors_list = [s for s in Globs.SECTOR_DIC.keys() if s not in beta_list]
        market_cap_list = ['VN30', 'VNMID', 'VNSML', 'other']



        return {
            'stocks': Globs.STOCKS,
            'available_day': Adapters.get_data_days(),
            'available_timeframe': Globs.ROLLING_TIMEFRAME,
            'rolling_method': Globs.ROLLING_METHOD,
            'group_dic': Globs.SECTOR_DIC,
            'market_stats': Globs.MAKETSTATS_SRC,
            'stock_stats': Globs.STOCKSTATS_SRC,
            'group_stats': Globs.GROUP_SRC,
            'daily_index_stats': ['Open', 'High', 'Low', 'Close', 'Volume', 'Value'],
            'stock_count_lines': ['count', 'consCount', 'percentage', 'percentile'],
            'stock_count_funcs': general_funcs + ohlcv_funcs,
            'count_line_funcs': general_funcs,
            'group_funcs': general_funcs,
            'other_funcs': general_funcs,
            'index_funcs': general_funcs + ohlcv_funcs,
            'tooltips': tooltips,
            'stock_filter': {
                'beta': beta_list,
                'sector': sectors_list,
                'marketCapIndex': market_cap_list
            }
        }

    @staticmethod
    def group_conditions(conditions_params: dict):
        """
            Process strategies that use stock data.
            
            Args:
                conditions_params: List of strategies with stocks
                
            Returns:
                pd.Series of boolean signals
        """
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
                        'line1': 'bid',
                        'line2': 'ask',
                        'stocks': ['HPG', 'SSI', 'NVL']
                    },
                    'params': {
                        'direction': 'above'
                    }
                },
                {
                    'function': 'two_line_pos',
                    'inputs': {
                        'line1': 'bu',
                        'line2': 'sd',
                        'stocks': ['SSI', 'NVL', "VN30"],
                    },
                    'params': {
                        'direction': 'crossover'
                    }
                },
            ]

        
        if not conditions_params:
            return None
            
        # Load stock data
        required_data, updated_params = CombiConds.load_and_process_group_data(conditions_params, use_sample_data=USE_SAMPLE_DATA)
        
        # Generate signals
        signals = CombiConds.combine_conditions(required_data, updated_params)

        return signals

    @staticmethod
    def other_conditions(conditions_params: dict):
        """
            Process strategies that use one series data.
            
            Args:
                conditions_params: List of strategies without stocks
                
            Returns:
                pd.Series of boolean signals
        """
        if not conditions_params:
            return None
            
        # Load one series data
        required_data, updated_params = CombiConds.load_and_process_one_series_data(conditions_params, use_sample_data=USE_SAMPLE_DATA)
        
        # Generate signals
        signals = CombiConds.combine_conditions(required_data, updated_params)
        
        return signals
    
    @staticmethod
    def index_daily_conditions(conditions_params: dict, index=None):
        """
            Process strategies that use one series data.
            
            Args:
                conditions_params: List of strategies without stocks
                
            Returns:
                pd.Series of boolean signals
        """
        if not conditions_params:
            return None
        
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
            
        # Load one series data
        required_data, updated_params = CombiConds.load_and_process_one_series_data(conditions_params, data_src='daily_index', use_sample_data=USE_SAMPLE_DATA)
        
        # Generate signals
        matched: pd.Series = CombiConds.combine_conditions(required_data, updated_params)

        if matched is None:
            return matched
        
        if isinstance(matched, pd.Series):
            df = matched.to_frame('signal')
        else:
            df = matched.copy()
            df.columns = ['signal']


        df['signal'] = df['signal'].shift(1).fillna(False)
        df['datetime'] = pd.to_datetime(df.index + ' ' + '09:15:00', format = '%Y_%m_%d %H:%M:%S')
        df['timestamp'] = df['datetime'].astype(int)

        signal = df.set_index('timestamp')['signal']

        full_index = pd.DataFrame(index)
        full_index = full_index.sort_values('candleTime')
        full_index['signal'] = full_index['candleTime'].map(signal).ffill()

        full_index['date'] = pd.to_datetime(full_index['candleTime']).dt.date
        full_index[full_index['signal']]['date'].unique()

        df[df['signal']].tail(20)

        return full_index.set_index('candleTime')['signal']

    @staticmethod
    def _combine_with_daily_index(intraday_signals: pd.Series=None, daily_signals: pd.Series=None) -> pd.Series:
        """Combine intraday signals with daily index signals."""
        if intraday_signals is None:
            return daily_signals
        
        if daily_signals is None:
            return intraday_signals

        intraday_signals.name = 'intraday'
        daily_signals.name = 'daily'
        
        combined = pd.concat([intraday_signals, daily_signals], axis=1)
        combined['date'] = pd.to_datetime(combined.index)
        combined = combined.sort_index()
        combined = combined[combined['intraday'].notna()]
        combined['daily'] = combined['daily'].ffill()
        
        return combined['intraday'] & combined['daily']

    @staticmethod
    def enter_conditions(conditions_params: list):
        def test():
            conditions_params = [
                {
                    "function": "absolute_change_in_range",
                    "inputs": {
                        "src": "pslab/stockcount/622096f2b313056b1f6d2c44b4f59fc3f34c0dacf7408adc727898f0cedd0564",

                    },
                    "params": {
                        "n_bars": 20,
                        "lower_thres": 10,
                        "upper_thres": 999,
                        "use_as_lookback_cond": False,
                        "lookback_cond_nbar": 5
                    }
                }
            ]

        def _load_and_process_counted_data(conditions_params: dict):
            required_data = {}  # Will store all needed data series/frames
            original_cols = set()  # Track original redis keys
            rolling_cols = set()  # Track (key, timeframe) combinations needed
            
            # First pass: Analyze data requirements and update conditions_params
            updated_conditions = []
            for condition in conditions_params:
                rolling_window = condition['inputs'].get('rolling_window')
                rolling_tf = condition['inputs'].get('rolling_timeframe')
                rolling_method = condition['inputs'].get('rolling_method', 'sum')
            
                
                # Create new condition with updated column names
                new_condition = {'function': condition['function'], 'inputs': {}, 'params': condition['params']}
                
                for param_name, value in condition['inputs'].items():
                    if value == "":
                        raise InputSourceEmptyError(f"Input {param_name} is empty!")
                    if param_name not in ['rolling_timeframe', 'rolling_window', 'stocks', 'rolling_method']:
                        redis_key = value  # Now expecting redis key instead of full condition params
                        
                        if rolling_tf:
                            rolling_tf_val = rolling_window if rolling_window is not None else Utils.convert_timeframe_to_rolling(rolling_tf)
                            rolling_cols.add((redis_key, rolling_tf_val, rolling_method))
                            new_key = f"{redis_key}_{rolling_tf_val}_{rolling_method}"
                        else:
                            original_cols.add(redis_key)
                            new_key = f"{redis_key}_None"
                        new_condition['inputs'][param_name] = new_key
                    else:
                        new_condition['inputs'][param_name] = condition['inputs'][param_name]
                
                updated_conditions.append(new_condition)
                    
            def get_redis_source(src: str):
                key_split = src.split("-")
                if len(key_split) == 1:
                    redis_key = key_split[0]
                    src_name = 'count'
                else:
                    redis_key = key_split[0]
                    src_name = key_split[1]

                if not redis_handler.check_exist(redis_key):
                    raise NoRedisDataError(f"Data not found in Redis for key: {redis_key}")
                return redis_handler.get_key(redis_key, pickle_data=True)[src_name]


            # Add original data
            for src in original_cols:
                key = f"{src}_None"
                required_data[key] = get_redis_source(src)
            
            # Add rolled data
            for src, tf, method in rolling_cols:
                key = f"{src}_{tf}_{method}"
                data: pd.DataFrame = get_redis_source(src)
                required_data[key] = Ta.apply_rolling(data, tf, method)

            return required_data, updated_conditions

        required_data, updated_conditions = _load_and_process_counted_data(conditions_params)

        if required_data is None:
            return None

        matched = CombiConds.combine_conditions(required_data, updated_conditions)

        return matched

    def filter_stocks( filter_criteria: Dict[str, List[str]], use_sample_data: bool = False) -> pd.DataFrame:
        """
        Filter stocks based on multiple criteria where each criterion is applied with OR logic within
        the same field and AND logic between different fields.
        
        Parameters:
        -----------
        df : pandas.DataFrame
            DataFrame containing stock information with columns including 'stock', 'sector',
            'beta', 'marketCapIndex'
        filter_criteria : Dict[str, List[str]]
            Dictionary where keys are column names and values are lists of acceptable values
            Example:
            filter_criteria = {
                "sector": ["VN30", "SOBank"],
                "beta": ["Low", "Medium"],
                "marketCapIndex": ["VN30", "VNSML"]
            }
        
        Returns:
        --------
        pandas.DataFrame
            Filtered DataFrame containing only rows that match all criteria
        
        Raises:
        -------
        KeyError
            If any key in filter_criteria doesn't exist as a column in df
        ValueError
            If filter_criteria is empty or contains empty lists
        """

        def test():
            filter_criteria = {
                'sector': ['All'],
            }
        
        df = Adapters.load_stock_classification_from_plasma(load_sample=use_sample_data)

        # Create a copy to avoid modifying the original DataFrame
        df_processed = df.copy()
        
        # Split the sector column and explode the DataFrame
        df_processed['sector'] = df_processed['sector'].str.split(',')
        df_processed = df_processed.explode('sector')
        
        # Remove any leading/trailing whitespace from sectors
        df_processed['sector'] = df_processed['sector'].str.strip()

            # Input validation
        if not filter_criteria:
            return Globs.STOCKS
        
        # Check if all specified columns exist in DataFrame
        missing_cols = set(filter_criteria.keys()) - set(df.columns)
        if missing_cols:
            raise KeyError(f"Columns not found in DataFrame: {missing_cols}")
        
        # Check for empty filter lists
        empty_filters = [k for k, v in filter_criteria.items() if not v]
        if empty_filters:
            raise ValueError(f"Empty filter lists for columns: {empty_filters}")
        
        # Initialize mask with all True
        mask = pd.Series(True, index=df_processed.index)
        
        # Apply filters
        for column, values in filter_criteria.items():
            # For each column, create an OR condition for all values
            column_mask = df_processed[column].isin(values)
            # Combine with AND to the existing mask
            mask &= column_mask
        
        # Return filtered DataFrame
        return list(df_processed[mask]['stock'].unique())
    
def run_test():
    enter_conditions = {
        "countline_params": [
            {
            "function": "range_cond",
            "inputs": {
                "line": "pslab/stockcount/541e18c346a0bfc8cd84c51c92a40e467a927939dbd0767584e0fb1d09103905"
            },
            "params": {
                "lower_thres": 100,
                "upper_thres": 900,
                "use_as_lookback_cond": False,
                "lookback_cond_nbar": 5
            }
            }
        ],
        "group_params": [],
        "other_params": [],
        "dailyindex_params": [],
        "lookback_periods": 60,
        "index": "F1"
    }

    PSLabHelper.enter_conditions(enter_conditions)

#%%

router = APIRouter()

@router.post("/filter")
async def filter(
    filter: Optional[dict] = Body(None), 
    fields: Optional[dict] = Body(None),    
    sort: Optional[dict] = Body(None), 
    limit: Optional[int] = Body(None),
    skip: Optional[int] = Body(None),
    authorization = Depends(reusable_oauth2)): 
    user = checkAuthen(authorization)
    try:
        if(filter is None): filter = {}
        queryData = filter
        
        # Add your query logic here if needed
        data = []  # Replace with actual data query
        total = len(data)
        
        if(limit is not None or skip is not None):
            total = len(data)  # Replace with actual count query
        
        return Reply.make(True, 'Success', {
            'data': data,
            'total': total,
        })
    except Exception as e:
        raise HTTPException(501, str(e))

@router.post("/get_data_index")
async def get_data_index(
    start_day: str = Body(...),
    end_day: str = Body(...),
    authorization = Depends(reusable_oauth2)
): 
    user = checkAuthen(authorization)
    try:
        # Convert start and end days to timestamps
        start_timestamp = day_to_timestamp(start_day)
        end_timestamp = day_to_timestamp(end_day, is_end_day=True)
        
        # Load data from plasma with sample=True
        df = Adapters.load_index_ohlcv_from_plasma(name=None, load_sample=USE_SAMPLE_DATA)
        df = df.drop('date', axis=1)
        
        # Filter data between start and end timestamps
        df = df[(df.index >= start_timestamp) & (df.index <= end_timestamp)]
        
        # Reset index to include timestamp in records
        df = df.reset_index()
        
        # Convert to records format
        result = df.to_dict(orient='records')
        
        return Reply.make(True, 'Success', result)
    except Exception as e:
        exceptionInfo(e)
        raise HTTPException(501, str(e))
    
@router.get("/get_functions")
async def get_functions(authorization = Depends(reusable_oauth2)):
    user = checkAuthen(authorization)
    try:
        functions = PSLabHelper.convert_function_mapping()
        return Reply.make(True, 'Success', functions)
    except Exception as e:
        exceptionInfo(e)
        raise HTTPException(501, str(e))
    
@router.get("/get_input_sources_functions")
async def get_functions(authorization = Depends(reusable_oauth2)):
    user = checkAuthen(authorization)
    try:
        functions = PSLabHelper.convert_input_function_mapping()
        return Reply.make(True, 'Success', functions)
    except Exception as e:
        exceptionInfo(e)
        raise HTTPException(501, str(e))

@router.get("/get_dic_info")
async def get_functions(authorization = Depends(reusable_oauth2)):
    user = checkAuthen(authorization)
    try:
        dic_info = PSLabHelper.get_dic_info()
        return Reply.make(True, 'Success', dic_info)
    except Exception as e:
        exceptionInfo(e)
        raise HTTPException(501, str(e))

@router.post("/stock_count_conditions")
async def stock_count_conditions(
    stocks: list = Body(...),
    line: str = Body('count'),
    count_smoothlen: int = Body(1),
    count_dups: bool = Body(False),
    cons_nbar: int = Body(10),
    percentage_smoothlen: int = Body(5),
    percentile_lookback_nbar: int = Body(256),
    conditions_params: list[dict] = Body(...),
    start_day: str = Body("2024_12_16"),
    end_day: str = Body("2024_12_17"),
    authorization = Depends(reusable_oauth2)
): 
    user = checkAuthen(authorization)
    def test():
        conditions = {
            "count_smoothlen": 1,
            "percentage_smoothlen": 5,
            "percentile_lookback_nbar": 256,
            "cons_nbar": 5,
            "count_dups": False,
            "line": "count",
            "conditions_params": [
                {
                    "function": "absolute_change_in_range",
                    "inputs": {
                        "src": "close"
                    },
                    "params": {
                        "n_bars": 20,
                        "lower_thres": 1,
                        "upper_thres": 999,
                        "use_as_lookback_cond": False,
                        "lookback_cond_nbar": 5
                    }
                }
            ],
            "start_day": "2025_01_06",
            "end_day": "2025_01_13",
            "stocks": [
                "ACB",
                "BCM",
                "BID",
                "BVH",
                "CTG",
                "FPT",
                "GAS",
                "GVR",
                "HDB",
                "HPG",
                "MBB",
                "MSN",
                "MWG",
                "PLX",
                "POW",
                "SAB",
                "SHB",
                "SSB",
                "SSI",
                "STB",
                "TCB",
                "TPB",
                "VCB",
                "VHM",
                "VIB",
                "VIC",
                "VJC",
                "VNM",
                "VPB",
                "VRE"
            ]
        }

        stocks: list = conditions['stocks']
        line: str = conditions['line']
        count_smoothlen: int = conditions['count_smoothlen']
        count_dups: bool = conditions['count_dups']
        cons_nbar: int = conditions['cons_nbar']
        percentage_smoothlen: int = conditions['percentage_smoothlen']
        percentile_lookback_nbar: int = conditions['percentile_lookback_nbar']
        conditions_params =  conditions['conditions_params']
        start_day: str = conditions['start_day']
        end_day: str = conditions['end_day']



    try:
        start_timestamp = day_to_timestamp(start_day)
        end_timestamp = day_to_timestamp(end_day, is_end_day=True)

        # Calculate stock count based on conditions
        count_line = {
            'count_smoothlen': count_smoothlen, 
            'count_dups': count_dups, 
            'cons_nbar': cons_nbar, 
            'percentage_smoothlen': percentage_smoothlen, 
            'percentile_lookback_nbar': percentile_lookback_nbar,
            'line': line,
            'stocks': stocks,
            'conditions_params': conditions_params
        }
        
        # Generate redis key for this condition
        redis_key = PSLabHelper.create_condition_hash_key(count_conditions=count_line)

        # Calculate and store in redis if not exists
        if not redis_handler.check_exist(redis_key):
            df_count = PSLabHelper.stock_count(
                conditions_params=conditions_params,
                stocks=stocks,
                count_smoothlen=count_smoothlen,
                count_dups=count_dups,
                cons_nbar=cons_nbar,
                percentage_smoothlen=percentage_smoothlen,
                percentile_lookback_nbar=percentile_lookback_nbar,
                line=line
            )
            redis_handler.set_key_with_ttl(redis_key, df_count, pickle_data=True)
        else:
            df_count = redis_handler.get_key(redis_key, pickle_data=True)

        df_count = df_count[(df_count.index >= start_timestamp) & (df_count.index <= end_timestamp)]

        df_count = df_count.fillna(0).reset_index()

        result = {
            'data': df_count.to_dict(orient='records'),
            'redis_key': redis_key  # Return redis key to client
        }
        
        return Reply.make(True, 'Success', result)
    except InputSourceEmptyError as e:
        return Reply.make(False, 'Empty Input Source!', {
            'error': str(e),
            'error_type': 'EMPTY_INPUT_SOURCE'
        })
    except Exception as e:
        exceptionInfo(e)
        raise HTTPException(501, str(e))


@router.post("/enter_conditions")
async def enter_conditions(
    countline_params: list[dict] = Body(...),
    group_params: list[dict] = Body(...),
    other_params: list[dict] = Body(...),
    dailyindex_params:list[dict] = Body(...),
    lookback_periods: int = Body(5),
    index: str = Body("F1"),
    authorization = Depends(reusable_oauth2)
): 
    def test():
        #%%

        enter_conditions = {
            "countline_params": [
                {
                    "function": "absolute_change_in_range",
                    "inputs": {
                        "src": "pslab/stockcount/f87d22879d7fab6b9bfd1e6cf97d220a1491ad5470d21c7d1f5b481793501ac0",
                        "rolling_timeframe": "30S",
                        "rolling_method": "sum"
                    },
                    "params": {
                        "n_bars": 1,
                        "lower_thres": -999,
                        "upper_thres": 999,
                        "use_as_lookback_cond": False,
                        "lookback_cond_nbar": 5
                    }
                }
            ],
            "group_params": [],
            "other_params": [],
            "dailyindex_params": [],
            "lookback_periods": 60,
            "index": "F1"
        }
        # dftest = redis_handler.get_key("pslab/stockcount/9564bcc35ccec1c9feaaf5fdbdfcbfa1bb4ba6230ed0ce05b955f33ad11cff99", pickle_data=True)
        # dftest['date'] = pd.to_datetime(dftest.index)

        countline_params = enter_conditions['countline_params']
        group_params = enter_conditions['group_params']
        other_params = enter_conditions['other_params']
        dailyindex_params = enter_conditions['dailyindex_params']

        lookback_periods = enter_conditions['lookback_periods']
        index = enter_conditions['index']
        #%%

    user = checkAuthen(authorization)
    try:
        # Load OHLC data first to get time indices
        df_ohlc = Adapters.load_index_ohlcv_from_plasma(name=index, load_sample=USE_SAMPLE_DATA)
        
        # Ensure df_ohlc has the required columns
        required_columns = ['open', 'high', 'low', 'close', 'day']
        if not all(col in df_ohlc.columns for col in required_columns):
            return Reply.make(False, f'Missing required OHLC columns for index {index}', None)

        # Get matched signals
        matched_countline = PSLabHelper.enter_conditions(countline_params)
        matched_group = PSLabHelper.group_conditions(group_params)
        matched_other = PSLabHelper.other_conditions(other_params)
        index_daily_signals = PSLabHelper.index_daily_conditions(dailyindex_params, index=df_ohlc.index)
        matched = Utils.and_conditions([matched_countline, matched_group, matched_other, index_daily_signals])

        # matched = PSLabHelper._combine_with_daily_index(intraday_signals, index_daily_signals)
        if matched is None:
            matched = Utils.new_1val_series(True, df_ohlc)

        if isinstance(matched, pd.DataFrame):
            if 0 in matched.columns:
                matched = matched[0]

        enter_signals = matched[matched]

        # Generate exit signals based on lookback_periods
        exit_signals = matched.shift(lookback_periods).fillna(False)
        exit_signals = exit_signals[exit_signals]

        # Initialize ReturnStats configuration
        config = ReturnStatsConfig(
            lookback_periods=lookback_periods,
            use_pct=False
        )

        # Calculate returns and statistics
        df_returns = ReturnStats.calculate_returns(
            df_ohlc=df_ohlc,
            signals=matched,
            config=config
        )

        # Get statistics and handle None/NaN values
        stats = ReturnStats.get_statistics(df_returns)
        trade_summary = ReturnStats.get_trade_summary(df_returns)

        # Prepare the response with cleaned data
        result = {
            'signals': {
                'enter': enter_signals.to_dict(),
                'exit': exit_signals.to_dict(),
            },
            'returns': {
                'daily_cumulative': PSLabHelper.clean_for_json(stats['daily']['cum_return']),
                'yearly': PSLabHelper.clean_for_json(stats['yearly']),
                'monthly': PSLabHelper.clean_for_json(stats['monthly'])
            },
            'summary': PSLabHelper.clean_for_json(trade_summary)
        }

        return Reply.make(True, 'Success', result)

    except NoRedisDataError as e:
        return Reply.make(False, 'Data Not Found', {
            'error': str(e),
            'error_type': 'NO_REDIS_DATA'
        })
    except InputSourceEmptyError as e:
        return Reply.make(False, 'Empty Input Source!', {
            'error': str(e),
            'error_type': 'EMPTY_INPUT_SOURCE'
        })
    except Exception as e:
        exceptionInfo(e)
        raise HTTPException(501, str(e))
    
# @router.post("/plot_charts")
# async def plot_charts_old(
#     group_params: list[dict] = Body(...),
#     other_params: list[dict] = Body(...),
#     dailyindex_params: list[dict] = Body(...),
#     start_day: str = Body(...),
#     end_day: str = Body(...),
#     authorization = Depends(reusable_oauth2)
# ): 
#     user = checkAuthen(authorization)

#     def test():
#         #%%
#         conditions_params = {
#             "group_params": [
#                 {
#                     "function": "MA",
#                     "inputs": {
#                         "src": "bu2",
#                         "stocks": ["All"]
#                     },
#                     "params": {
#                         "window": 10,
#                         "ma_type": "EMA"
#                     },
#                     "outputs": {
#                         "output": {
#                             "type": "line",
#                             "mapping": "MA",
#                             "value": "MA",
#                             "color": "#92c12e"
#                         }
#                     }
#                 }
#             ],
#             "other_params": [
#                 {
#                     "function": "MA",
#                     "inputs": {
#                         "src": "Arbit"
#                     },
#                     "params": {
#                         "window": 30,
#                         "ma_type": "EMA"
#                     },
#                     "outputs": {
#                         "output": {
#                             "type": "line",
#                             "mapping": "MA",
#                             "value": "MA2",
#                             "color": "#0c8fbd"
#                         }
#                     }
#                 }
#             ],
#             "dailyindex_params": [
#                 {
#                     "function": "MA",
#                     "inputs": {
#                         "src": "F1Close"
#                     },
#                     "params": {
#                         "window": 10,
#                         "ma_type": "EMA"
#                     },
#                     "outputs": {
#                         "output": {
#                             "type": "line",
#                             "mapping": "MA",
#                             "value": "MA3",
#                             "color": "#c027a1"
#                         }
#                     }
#                 }
#             ],
#             "start_day": "2025_01_13",
#             "end_day": "2025_01_14"
#         }

#         group_params = conditions_params['group_params']
#         other_params = conditions_params['other_params']
#         dailyindex_params = conditions_params['dailyindex_params']
#         start_day = conditions_params['start_day']
#         end_day = conditions_params['end_day']
#         #%%

#     try:

#         data, data_info = QuerryData.load_all_data(group_params, other_params, dailyindex_params, start_day, end_day)
#         result = {
#             "data": data,
#             "data_info": data_info
#         }
#         return Reply.make(True, 'Success',result)
#     except Exception as e:
#         exceptionInfo(e)
#         raise HTTPException(501, str(e))

@router.post("/plot_charts")
async def plot_charts(
    group_params: list[dict] = Body(...),
    other_params: list[dict] = Body(...),
    dailyindex_params: list[dict] = Body(...),
    start_day: str = Body(...),
    end_day: str = Body(...),
    authorization = Depends(reusable_oauth2)
): 
    user = checkAuthen(authorization)
    def test():
        #%%
        conditions_params = {
            "group_params": [],
            "other_params": [],
            "dailyindex_params": [
                {
                    "function": "squeeze",
                    "inputs": {
                        "src": "F1Open",
                        "high": "F1High",
                        "low": "F1Low",
                        "close": "F1Close",
                        "rolling_window": 30,
                        "rolling_method": "median"
                    },
                    "outputs": {
                        "output1": {
                            "type": "bool",
                            "mapping": "sqz_on",
                            "value": "sqz_on",
                            "color": "#8a2466"
                        },
                        "output2": {
                            "type": "bool",
                            "mapping": "sqz_off",
                            "value": "sqz_off",
                            "color": "#8d4139"
                        }
                    },
                    "params": {
                        "bb_length": 20,
                        "length_kc": 20,
                        "mult_kc": 1.5,
                        "use_true_range": False
                    }
                }
            ],
            "start_day": "2025_01_14",
            "end_day": "2025_01_21"
        }

        group_params = conditions_params['group_params']
        other_params = conditions_params['other_params']
        dailyindex_params = conditions_params['dailyindex_params']
        start_day = conditions_params['start_day']
        end_day = conditions_params['end_day']
        #%%
    try:

        # Validate unique output values across all parameter groups
        all_params = group_params + other_params + dailyindex_params

        validate_unique_output_values(all_params)

        # Your existing logic
        data, data_info = QuerryData.load_all_data(group_params, other_params, dailyindex_params, start_day, end_day)
        result = {
            "data": data,
            "data_info": data_info
        }
        return Reply.make(True, 'Success', result)
    except DuplicateValueError as e:
        raise HTTPException(
            status_code=422,
            detail={
                'error': 'Tên chart bị trùng, hãy đặt tên lại!',
                'duplicates': e.duplicate_values
            }
        )
    except Exception as e:
        exceptionInfo(e)
        raise HTTPException(501, str(e))

@router.post("/filter_stock")
async def filter_stock(
    filter_criteria: dict = Body(...),
    authorization = Depends(reusable_oauth2)
): 
    user = checkAuthen(authorization)
    try:
        result = PSLabHelper.filter_stocks(filter_criteria, use_sample_data = USE_SAMPLE_DATA)
        return Reply.make(True, 'Success', result)
    except Exception as e:
        exceptionInfo(e)
        raise HTTPException(501, str(e))
    
