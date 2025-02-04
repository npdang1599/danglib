from abc import ABC, abstractmethod
from typing import Dict, Tuple, Set, List, Union
from dataclasses import dataclass
from copy import deepcopy
import pandas as pd
from danglib.pslab.resources import Adapters, Globs
from danglib.pslab.utils import Utils

class InputSourceEmptyError(Exception):
    """Raise when Input sources are empty"""
    pass


@dataclass
class DataRequest:
    """Data request configuration"""
    original_cols: Set[str] = None
    rolling_cols: Set[Union[Tuple[str, int], Tuple[str, int, int]]] = None
    conditions: List[dict] = None
    stocks: List[str] = None

class BaseDataLoader(ABC):
    """Base class for data loading and processing"""
    
    def process_conditions(self, conditions_params: List[dict]) -> DataRequest:
        """Process conditions to determine required data"""
        request = DataRequest(set(), set(), [])
        
        for condition in conditions_params:
            rolling_tf = condition['inputs'].get('rolling_timeframe')
            stocks = condition['inputs'].get('stocks', Globs.STOCKS)
            stocks_key = hash('_'.join(sorted(stocks))) if stocks else None
            
            new_condition = {
                'function': condition['function'],
                'inputs': {},
                'params': condition['params']
            }
            
            for param_name, col_name in condition['inputs'].items():
                if param_name not in ['rolling_timeframe', 'stocks']:
                    if not col_name:
                        raise InputSourceEmptyError(f"Input {param_name} is empty!")
                        
                    if rolling_tf:
                        rolling_tf_val = Utils.convert_timeframe_to_rolling(rolling_tf)
                        key = self._get_rolling_key(col_name, rolling_tf_val, stocks_key)
                        request.rolling_cols.add(self._get_rolling_tuple(col_name, rolling_tf_val, stocks_key))
                    else:
                        key = self._get_original_key(col_name, stocks_key)
                        request.original_cols.add(self._get_original_tuple(col_name, stocks_key))
                        
                    new_condition['inputs'][param_name] = key
                else:
                    new_condition['inputs'][param_name] = condition['inputs'][param_name]
                    
            request.conditions.append(new_condition)
            
        return request

    @abstractmethod
    def _get_rolling_tuple(self, col_name: str, rolling_tf: int, stocks_key: int = None) -> tuple:
        """Get tuple format for rolling columns"""
        pass

    @abstractmethod
    def _get_original_tuple(self, col_name: str, stocks_key: int = None) -> tuple:
        """Get tuple format for original columns"""
        pass

    @abstractmethod
    def _get_rolling_key(self, col_name: str, rolling_tf: int, stocks_key: int = None) -> str:
        """Get key for rolling data"""
        pass

    @abstractmethod
    def _get_original_key(self, col_name: str, stocks_key: int = None) -> str:
        """Get key for original data"""
        pass

    @abstractmethod
    def _load_data(self, request: DataRequest) -> pd.DataFrame:
        """Load required data"""
        pass

    def load_and_process(self, conditions_params: List[dict], **kwargs) -> Tuple[Dict[str, pd.DataFrame], List[dict]]:
        """Main method to load and process data"""
        request = self.process_conditions(conditions_params)
        data = self._load_data(request, **kwargs)
        
        required_data = {}
        
        # Process original columns
        for col_tuple in request.original_cols:
            key = self._get_original_key(*col_tuple)
            col = col_tuple[0] if len(col_tuple) == 1 else col_tuple[0]
            required_data[key] = data[col]
            
        # Process rolling columns
        for col_tuple in request.rolling_cols:
            key = self._get_rolling_key(*col_tuple)
            col = col_tuple[0]
            tf = col_tuple[1]
            required_data[key] = data[col].rolling(tf).sum()
            
        return required_data, request.conditions

class GroupDataLoader(BaseDataLoader):
    """Data loader for group data"""
    
    def _get_rolling_tuple(self, col_name: str, rolling_tf: int, stocks_key: int) -> tuple:
        return (col_name, rolling_tf, stocks_key)
        
    def _get_original_tuple(self, col_name: str, stocks_key: int) -> tuple:
        return (col_name, stocks_key)
        
    def _get_rolling_key(self, col_name: str, rolling_tf: int, stocks_key: int) -> str:
        return f"{col_name}_{rolling_tf}_{stocks_key}"
        
    def _get_original_key(self, col_name: str, stocks_key: int) -> str:
        return f"{col_name}_None_{stocks_key}"
        
    def _load_data(self, request: DataRequest, use_sample_data: bool = False) -> pd.DataFrame:
        all_cols_by_stocks = {}
        
        # Group columns by stocks
        for col, stocks_key in request.original_cols:
            if stocks_key not in all_cols_by_stocks:
                all_cols_by_stocks[stocks_key] = set()
            all_cols_by_stocks[stocks_key].add(col)
            
        for col, _, stocks_key in request.rolling_cols:
            if stocks_key not in all_cols_by_stocks:
                all_cols_by_stocks[stocks_key] = set()
            all_cols_by_stocks[stocks_key].add(col)

        # Load data for each stocks combination
        all_data = {}
        for stocks_key, cols in all_cols_by_stocks.items():
            filtered_stocks = self._get_stocks_for_key(stocks_key, request.conditions)
            data = Adapters.load_groups_and_stocks_data_from_plasma(
                list(cols), filtered_stocks, use_sample_data
            )
            all_data[stocks_key] = data.groupby(level=0, axis=1).sum()
            
        return all_data

    def _get_stocks_for_key(self, stocks_key: int, conditions: List[dict]) -> List[str]:
        """Get original stocks list for a given key"""
        for condition in conditions:
            stocks = condition['inputs'].get('stocks', Globs.STOCKS)
            if hash('_'.join(sorted(stocks))) == stocks_key:
                return stocks
        return Globs.STOCKS

class SingleSeriesDataLoader(BaseDataLoader):
    """Data loader for single series data"""
    
    def _get_rolling_tuple(self, col_name: str, rolling_tf: int, stocks_key=None) -> tuple:
        return (col_name, rolling_tf)
        
    def _get_original_tuple(self, col_name: str, stocks_key=None) -> tuple:
        return (col_name,)
        
    def _get_rolling_key(self, col_name: str, rolling_tf: int, stocks_key=None) -> str:
        return f"{col_name}_{rolling_tf}"
        
    def _get_original_key(self, col_name: str, stocks_key=None) -> str:
        return f"{col_name}_None"
        
    def _load_data(self, request: DataRequest, data_src: str = 'market_stats', 
                  use_sample_data: bool = False) -> pd.DataFrame:
        all_cols = {col for col, in request.original_cols} | \
                  {col for col, _ in request.rolling_cols}
                  
        if data_src == 'market_stats':
            return Adapters.load_market_stats_from_plasma(list(all_cols), use_sample_data)
        return Adapters.load_index_daily_ohlcv_from_plasma(list(all_cols), use_sample_data)

class StockDataLoader(BaseDataLoader):
    """Data loader for stock data"""
    
    def _get_rolling_tuple(self, col_name: str, rolling_tf: int, stocks_key=None) -> tuple:
        return (col_name, rolling_tf)
        
    def _get_original_tuple(self, col_name: str, stocks_key=None) -> tuple:
        return (col_name,)
        
    def _get_rolling_key(self, col_name: str, rolling_tf: int, stocks_key=None) -> str:
        return f"{col_name}_{rolling_tf}"
        
    def _get_original_key(self, col_name: str, stocks_key=None) -> str:
        return f"{col_name}_None"
        
    def _load_data(self, request: DataRequest, use_sample_data: bool = False) -> pd.DataFrame:
        all_cols = {col for col, in request.original_cols} | \
                  {col for col, _ in request.rolling_cols}
        return Adapters.load_stock_data_from_plasma(
            list(all_cols), stocks=request.stocks, load_sample=use_sample_data
        )

class DataLoaderFactory:
    """Factory for creating data loaders"""
    
    @staticmethod
    def create_loader(loader_type: str) -> BaseDataLoader:
        loaders = {
            'group': GroupDataLoader,
            'single_series': SingleSeriesDataLoader,
            'stock': StockDataLoader
        }
        return loaders[loader_type]()

# Update CombiConds class methods
class CombiConds:
    @staticmethod
    def load_and_process_group_data(conditions_params: List[dict], use_sample_data=False):
        loader = DataLoaderFactory.create_loader('group')
        return loader.load_and_process(conditions_params, use_sample_data=use_sample_data)
        
    @staticmethod
    def load_and_process_one_series_data(conditions_params: List[dict], 
                                       data_src: str = 'market_stats',
                                       use_sample_data=False):
        loader = DataLoaderFactory.create_loader('single_series')
        return loader.load_and_process(conditions_params, 
                                     data_src=data_src,
                                     use_sample_data=use_sample_data)
        
    @staticmethod
    def load_and_process_stock_data(conditions_params: List[dict], 
                                  stocks: List[str] = None,
                                  use_sample_data=False):
        loader = DataLoaderFactory.create_loader('stock')
        return loader.load_and_process(conditions_params,
                                     stocks=stocks, 
                                     use_sample_data=use_sample_data)