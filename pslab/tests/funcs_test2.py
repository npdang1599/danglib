from danglib.pslab.funcs import *

class CombiConds2:
    INPUTS_SIDE_PARAMS = ['timeframe', 'stocks', 'rolling_window', 'rolling_method', 'daily_rolling', 'exclude_atc']
    VALID_SOURCES = ['group', 'market_stats', 'daily_index', 'stock']

    @staticmethod
    def _create_data_key(col_name, timeframe, stocks_key, rolling_window, rolling_method, daily_rolling, exclude_atc) -> str:
        """Create a unique key for a data column based on processing parameters."""
        key_parts = [col_name, timeframe, str(stocks_key), str(rolling_window), rolling_method,  str(daily_rolling), str(exclude_atc)]
        return "_".join(key_parts)

    @staticmethod
    def _collect_required_columns(conditions_params: list[dict]) -> tuple[set[tuple], list[dict]]:
        def test():
            conditions_params = TestSets.LOAD_PROCESS_SERIES_DATA

        """Collect required columns and build updated conditions with resolved inputs."""
        required_cols = {}
        updated_conditions = []
        stocks_key_map = {}
        
        for condition in conditions_params:
            # Get processing parameters with defaults
            side_params_dict = {
                "timeframe" : condition['inputs'].get('timeframe', Globs.BASE_TIMEFRAME),
                "rolling_window" : condition['inputs'].get('rolling_window'),
                "rolling_method" : condition['inputs'].get('rolling_method', 'sum'),
                "daily_rolling" : condition['inputs'].get('daily_rolling', True),
                "exclude_atc" : condition['inputs'].get('exclude_atc', False),
            }
            
            stocks = condition['inputs'].get('stocks', None)
            # Generate stock key if needed
            stocks_key = None
            if stocks is not None:
                stocks_key = hash('_'.join(sorted(stocks)))

            side_params_dict['stocks_key'] = stocks_key

            if stocks_key not in stocks_key_map:
                stocks_key_map[stocks_key] = stocks
            
            new_condition = deepcopy(condition)
            new_condition['inputs'] = {}
            
            # Process each input parameter
            for param_name, col_name in condition['inputs'].items():
                if param_name not in CombiConds2.INPUTS_SIDE_PARAMS:
                    if not col_name:
                        raise InputSourceEmptyError(f"Input {param_name} is empty!")
                    
                    col_dict = side_params_dict.copy()
                    col_dict['col_name'] = col_name
                    data_key = CombiConds2._create_data_key(**col_dict)

                    if data_key not in required_cols:
                        required_cols[data_key] = col_dict
                    
                    # Create a unique key
                    new_condition['inputs'][param_name] = data_key
                else:
                    new_condition['inputs'][param_name] = condition['inputs'][param_name]
            
            updated_conditions.append(new_condition)
            
        return required_cols, updated_conditions, stocks_key_map
    
    @staticmethod
    def _process_data_series(data: pd.DataFrame, col_name: str, timeframe: str,
                           rolling_window: int, rolling_method: str, daily_rolling: bool, exclude_atc: bool, 
                           allow_remove_atc: bool = True, allow_resample_candle: bool = True, **kwargs) -> pd.Series:
        
        def test():
            data = pd.DataFrame()
            params = {'timeframe': '30S',
                'rolling_window': 1,
                'rolling_method': 'sum',
                'daily_rolling': True,
                'exclude_atc': False,
                'stocks_key': None,
                'col_name': 'fF1BuyVol'}
            
            col_name = params['col_name']
            timeframe = params['timeframe']
            rolling_window = params['rolling_window']
            rolling_method = params['rolling_method']
            daily_rolling = params['daily_rolling']
            exclude_atc = params['exclude_atc']


        data_processed = data[col_name].copy()
        resampling_method = Resampler.get_agg_dict([col_name])[col_name]

        if exclude_atc and allow_remove_atc:
            def get_atc(data: PandasObject):
                """Extract ATC timestamps from the data."""
                atc_stamps = []
                
                # Get the index timestamps
                idx = data.index
                
                # Convert index directly to datetime objects for checking hour and minute
                datetimes = pd.DatetimeIndex(idx)
                
                # Find all timestamps at 14:45 (ATC)
                atc_mask = (datetimes.hour == 14) & (datetimes.minute == 45)
                atc_stamps = idx[atc_mask].tolist()
                
                return atc_stamps

            ATC_STAMPS = get_atc(data_processed)
            data_processed = data_processed[~data_processed.index.isin(ATC_STAMPS)]

        def resample_vn_stock_data(df: pd.DataFrame, timeframe: str, agg_method: dict) -> pd.DataFrame:
                    # Reset index để có thể xử lý candleTime
            df = df.reset_index()

            # Tính candleTime mới cho mỗi row
            df['candleTime'] = Resampler._calculate_candle_time(
                    timestamps=df['candleTime'],
                    timeframe=timeframe
                )
            
            # Group theo candleTime mới và áp dụng aggregation
            grouped = df.groupby('candleTime').agg(agg_method)

            return grouped
    
        # Apply timeframe resampling if needed
        if timeframe != Globs.BASE_TIMEFRAME and allow_resample_candle:
            data_processed = resample_vn_stock_data(
                data_processed, 
                timeframe=timeframe, 
                agg_method=resampling_method
            )

        # Apply rolling calculations if needed
        if rolling_window is not None:
            data_processed = Ta.apply_rolling(data_processed, rolling_window, rolling_method, daily_rolling)
        
        return data_processed
    
    @staticmethod
    def load_and_process_data(
        conditions_params: list[dict],
        data_source: str = 'group', 
        provided_data: pd.DataFrame = None,
        realtime: bool = False,
        stocks: list[str] = None
    )-> tuple[dict[str, pd.Series], list[dict]]:
        """Unified method to load and process data based on conditions parameters."""

        def test():
            conditions_params = TestSets.LOAD_PROCESS_SERIES_DATA
            data_source = 'market_stats'
            provided_data = None
            realtime = False
            stocks = None

        valid_sources = CombiConds2.VALID_SOURCES
        if data_source not in valid_sources:
            raise ValueError(f"data_source must be one of {valid_sources}")
        
        required_cols, updated_conditions, stocks_key_map = CombiConds2._collect_required_columns(conditions_params)
        required_data = {}

        if data_source in ['group']:
            all_cols_by_stocks: dict[int, set[str]] = {}
            for data_key, params in required_cols.items():
                col = params['col_name']
                stocks_key = params['stocks_key']

                if stocks_key not in all_cols_by_stocks:
                    all_cols_by_stocks[stocks_key] = set()
                all_cols_by_stocks[stocks_key].add(col)

            for stocks_key, cols in all_cols_by_stocks.items():
                # Find correct stock filter for this combination
                filtered_stocks = stocks_key_map[stocks_key]

                if provided_data is None:
                    if realtime:
                        data = Adapters.load_stock_data_from_plasma_realtime(list(cols), filtered_stocks)
                    else:
                        data = Adapters.load_groups_and_stocks_data_from_plasma(list(cols), filtered_stocks)
                else:
                    data = provided_data

                data = data.groupby(level=0, axis=1).sum()

                for data_key, params in required_cols.items():
                    if params['stocks_key'] == stocks_key and data_key not in required_data:
                        required_data[data_key] = CombiConds2._process_data_series(data, **params)

        elif data_source in ['market_stats', 'daily_index', 'stock']:
            unique_cols = {params['col_name'] for params in required_cols.values()}

            # Load data or use provided data
            if provided_data is None:
                if data_source in ['market_stats']:
                    if not realtime:
                        data = Adapters.load_market_stats_from_plasma(list(unique_cols))
                    else:
                        data = Adapters.load_market_stats_from_plasma_realtime(list(unique_cols))
                elif data_source in ['daily_index']:  # daily_index
                    data = Adapters.load_index_daily_ohlcv_from_plasma(list(unique_cols))
                elif data_source in ['stock']:
                    data = Adapters.load_stock_data_from_plasma(list(unique_cols), stocks=stocks)
            else:
                data = provided_data[list(unique_cols)]

            is_not_daily_index = data_source != 'daily_index'

            for data_key, params in required_cols.items():
                required_data[data_key] = CombiConds2._process_data_series(
                    data, **params, 
                    allow_resample_candle=is_not_daily_index, 
                    allow_remove_atc=is_not_daily_index
                )

        return required_data, updated_conditions


    # Wrapper methods for backward compatibility
    @staticmethod
    def load_and_process_group_data(conditions_params: list[dict],
                                  realtime=False) -> tuple[dict[str, pd.Series], list[dict]]:
        """Load and process group data (backward compatibility wrapper)."""
        return CombiConds2.load_and_process_data(
            conditions_params=conditions_params,
            data_source='group',
            realtime=realtime,
        )

    @staticmethod
    def load_and_process_one_series_data(conditions_params: list[dict], data_src: str = 'market_stats', realtime: bool = False,
                                       data: pd.DataFrame=None) -> tuple[dict[str, pd.Series], list[dict]]:
        """Load and process one series data (backward compatibility wrapper)."""
        return CombiConds2.load_and_process_data(
            conditions_params=conditions_params,
            data_source=data_src,
            provided_data=data,
            realtime=realtime
        )

    @staticmethod
    def load_and_process_stock_data(conditions_params: list[dict], stocks: list[str] = None) -> tuple[dict[str, pd.Series], list[dict]]:
        """Load and process stock data (backward compatibility wrapper)."""
        return CombiConds2.load_and_process_data(
            conditions_params=conditions_params,
            data_source='stock',
            stocks=stocks
        )

def new_version():
    required_data, conditions_params = CombiConds2.load_and_process_stock_data(TestSets.COMBINE_TEST)
    combine_calculator: str = 'and'
    matched = CombiConds.combine_conditions(required_data, conditions_params, combine_calculator)
    return matched

def old_version():
    required_data, conditions_params = CombiConds.load_and_process_stock_data(TestSets.COMBINE_TEST)
    combine_calculator: str = 'and'
    matched = CombiConds.combine_conditions(required_data, conditions_params, combine_calculator)
    return matched


def test():
    required_data, conditions_params = CombiConds2.load_and_process_group_data(TestSets.LOAD_PROCESS_GROUP_DATA)
    
    # Kết hợp điều kiện
    new_combined = CombiConds.combine_conditions(required_data=required_data, conditions_params=conditions_params, combine_calculator='and')

    for k, v in required_data.items():
        print(k, len(v))





        


