from danglib.pslab.funcs import *

class CombiConds2:
    INPUTS_SIDE_PARAMS = ['rolling_window', 'stocks', 'rolling_method', 'timeframe', 'daily_rolling', 'exclude_atc']

    @staticmethod
    def _collect_required_columns(conditions_params: list[dict]) -> tuple[set[tuple], list[dict]]:
        def test():
            conditions_params = TestSets.LOAD_PROCESS_GROUP_DATA

        """Collect required columns and build updated conditions with resolved inputs."""
        required_cols = set()
        updated_conditions = []
        
        for condition in conditions_params:
            # Get processing parameters with defaults
            timeframe = condition['inputs'].get('timeframe', Globs.BASE_TIMEFRAME)
            rolling_window = condition['inputs'].get('rolling_window')
            rolling_method = condition['inputs'].get('rolling_method', 'sum')
            daily_rolling = condition['inputs'].get('daily_rolling', True)
            exclude_atc = condition['inputs'].get('exclude_atc', False)
            stocks = condition['inputs'].get('stocks', Globs.STOCKS)
            
            # Generate stock key if needed
            stocks_key = hash('_'.join(sorted(stocks))) if 'stocks' in condition['inputs'] else None
            
            new_condition = deepcopy(condition)
            new_condition['inputs'] = {}
            
            # Process each input parameter
            for param_name, col_name in condition['inputs'].items():
                if param_name not in CombiConds2.INPUTS_SIDE_PARAMS:
                    if not col_name:
                        raise InputSourceEmptyError(f"Input {param_name} is empty!")
                    
                    # Create tuple of requirements based on data source
                    col_tuple = (
                        (col_name, timeframe, stocks_key, rolling_window, rolling_method, daily_rolling, exclude_atc)
                        if stocks_key is not None
                        else (col_name, timeframe, rolling_window, rolling_method, daily_rolling, exclude_atc)
                    )
                    
                    required_cols.add(col_tuple)
                    
                    # Create a unique key
                    key_parts = [col_name, timeframe]
                    if stocks_key is not None:
                        key_parts.append(str(stocks_key))
                    key_parts.extend([str(rolling_window), rolling_method, str(daily_rolling), str(exclude_atc)])
                    new_key = "_".join(key_parts)
                    
                    new_condition['inputs'][param_name] = new_key
                else:
                    new_condition['inputs'][param_name] = condition['inputs'][param_name]
            
            updated_conditions.append(new_condition)
            
        return required_cols, updated_conditions

    @staticmethod
    def _process_data_series(data: pd.DataFrame, col: str, timeframe: str, 
                           rolling_window: int, rolling_method: str, daily_rolling: bool, exclude_atc: bool) -> pd.Series:
        """Process a data series with timeframe resampling and rolling calculations."""
        data_processed = data[col].copy()

        if exclude_atc:
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
        
        # Apply timeframe resampling if needed
        if timeframe != Globs.BASE_TIMEFRAME:
            data_processed = Resampler.resample_vn_stock_data(
                data_processed.to_frame(), 
                timeframe=timeframe, 
                agg_dict=Resampler.get_agg_dict([col])
            )[col]
        
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
    ) -> tuple[dict[str, pd.Series], list[dict]]:
        """Unified method to load and process data based on conditions parameters."""
        def test():
            conditions_params = TestSets.LOAD_PROCESS_STOCKS_DATA
            data_source = 'stock'
            provided_data = None
            realtime = False
            stocks = None
        
        try:
            # Validate data source
            valid_sources = ['group', 'one_series', 'stock', 'market_stats', 'daily_index']
            if data_source not in valid_sources:
                raise ValueError(f"data_source must be one of {valid_sources}")

            # Collect required columns and build updated conditions
            required_cols, updated_conditions = CombiConds2._collect_required_columns(conditions_params)
            required_data = {}

            # Handle different data sources
            if data_source in ['group']:
                # Group columns by stock combinations for efficient data loading
                all_cols_by_stocks: dict[int, set[str]] = {}
                for col_tuple in required_cols:
                    if len(col_tuple) == 6:  # Stock-based tuple format
                        col, _, stocks_key, _, _, _, _ = col_tuple
                        if stocks_key not in all_cols_by_stocks:
                            all_cols_by_stocks[stocks_key] = set()
                        all_cols_by_stocks[stocks_key].add(col)

                # Process data for each stock combination
                for stocks_key, cols in all_cols_by_stocks.items():
                    # Find correct stock filter for this combination
                    filtered_stocks = next(
                        (condition['inputs'].get('stocks', Globs.STOCKS) 
                        for condition in conditions_params 
                        if 'stocks' in condition['inputs'] and
                        hash('_'.join(sorted(condition['inputs'].get('stocks', Globs.STOCKS)))) == stocks_key),
                        Globs.STOCKS
                    )

                    # Load data or use provided data
                    if provided_data is None:
                        if realtime:
                            data = Adapters.load_stock_data_from_plasma_realtime(list(cols), filtered_stocks)
                        else:
                            data = Adapters.load_groups_and_stocks_data_from_plasma(list(cols), filtered_stocks)
                    else:
                        data = provided_data

                    data = data.groupby(level=0, axis=1).sum()
                    
                    # Process each required column
                    for col_tuple in required_cols:
                        if len(col_tuple) == 6:  # Stock-based tuple format
                            col, timeframe, sk, rolling_window, rolling_method, daily_rolling, exclude_atc = col_tuple
                            if sk == stocks_key:
                                key = f"{col}_{timeframe}_{sk}_{rolling_window}_{rolling_method}_{daily_rolling}_{exclude_atc}"
                                required_data[key] = CombiConds2._process_data_series(
                                    data, col, timeframe, rolling_window, rolling_method, daily_rolling, exclude_atc
                                )
            
            elif data_source in ['one_series', 'market_stats', 'daily_index']:
                # Get unique columns needed
                unique_cols = {col for col, *_ in required_cols}
                
                # Load data or use provided data
                if provided_data is None:
                    if data_source in ['market_stats', 'one_series']:
                        if not realtime:
                            data = Adapters.load_market_stats_from_plasma(list(unique_cols))
                        else:
                            data = Adapters.load_market_stats_from_plasma_realtime(list(unique_cols))
                    else:  # daily_index
                        data = Adapters.load_index_daily_ohlcv_from_plasma(list(unique_cols))
                else:
                    data = provided_data[list(unique_cols)]
                
                # Process each required column
                for col_tuple in required_cols:
                    if len(col_tuple) == 5:  # One-series tuple format
                        col, timeframe, rolling_window, rolling_method, daily_rolling, exclude_atc = col_tuple
                        key = f"{col}_{timeframe}_{rolling_window}_{rolling_method}_{daily_rolling}_{exclude_atc}"

                        if exclude_atc:
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
                        
                        # Only resample market_stats data
                        should_resample = (timeframe != Globs.BASE_TIMEFRAME) and (data_source in ['market_stats', 'one_series'])
                        

                        if should_resample:
                            data_processed = Resampler.resample_vn_stock_data(
                                data[col].to_frame(), 
                                timeframe=timeframe,
                                agg_dict=Resampler.get_agg_dict([col])
                            )[col]
                        else:
                            data_processed = data[col].copy()
                        
                        # Apply rolling calculations if needed
                        if rolling_window is not None:
                            data_processed = Ta.apply_rolling(data_processed, rolling_window, rolling_method, daily_rolling)
                        
                        required_data[key] = data_processed
            
            else:  # stock
                # Get unique columns needed
                unique_cols = {col for col, *_ in required_cols}
                
                # Load stock data or use provided data
                if provided_data is None:
                    data = Adapters.load_stock_data_from_plasma(
                        list(unique_cols), 
                        stocks=stocks or Globs.STOCKS
                    )
                else:
                    data = provided_data
                
                # Process each required column
                for col_tuple in required_cols:
                    if len(col_tuple) == 5:  # Stock tuple format (without stocks_key)
                        col, timeframe, rolling_window, rolling_method, daily_rolling = col_tuple
                        key = f"{col}_{timeframe}_{rolling_window}_{rolling_method}_{daily_rolling}"
                        
                        # Resample all data if needed
                        if timeframe != Globs.BASE_TIMEFRAME:
                            resampled_data = Resampler.resample_vn_stock_data(
                                data, 
                                timeframe=timeframe,
                                agg_dict=Resampler.get_agg_dict(list(data.columns))
                            )
                            data_processed = resampled_data[col].copy()
                        else:
                            data_processed = data[col].copy()
                        
                        # Apply rolling calculations if needed
                        if rolling_window is not None:
                            data_processed = Ta.apply_rolling(data_processed, rolling_window, rolling_method, daily_rolling)
                        
                        required_data[key] = data_processed

            return required_data, updated_conditions
        
        except Exception as e:
            raise type(e)(f"Error in load_and_process_data ({data_source}): {str(e)}") from e

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
    
def test(conditions_params, data_source):
    def t():
        conditions_params = TestSets.LOAD_PROCESS_GROUP_DATA
        data_source = 'group'
    
    if data_source == 'group':
        required_data, updated_params = CombiConds2.load_and_process_group_data(conditions_params)
        required_data_old, updated_params_old = CombiConds.load_and_process_group_data(conditions_params)
    elif data_source == 'one_series':
        required_data, updated_params = CombiConds2.load_and_process_one_series_data(conditions_params)
        required_data_old, updated_params_old = CombiConds.load_and_process_one_series_data(conditions_params)
    elif data_source == 'stock':
        required_data, updated_params = CombiConds2.load_and_process_stock_data(conditions_params)
        required_data_old, updated_params_old = CombiConds.load_and_process_stock_data(conditions_params)

    def compare_results(required_data, updated_params, required_data_old, updated_params_old):
        print("Comparing results from CombiConds2 and CombiConds:")
        
        # Compare data dictionaries
        print("\nComparing data dictionaries:")
        if set(required_data.keys()) == set(required_data_old.keys()):
            print("✅ Both data dictionaries have the same keys")
        else:
            print("❌ Data dictionaries have different keys:")
            print(f"Keys only in CombiConds2: {set(required_data.keys()) - set(required_data_old.keys())}")
            print(f"Keys only in CombiConds: {set(required_data_old.keys()) - set(required_data.keys())}")
        
        # Compare data values
        for key in set(required_data.keys()).intersection(set(required_data_old.keys())):
            if required_data[key].equals(required_data_old[key]):
                print(f"✅ Series '{key}' match")
            else:
                print(f"❌ Series '{key}' differ:")
                print(f"  CombiConds2 shape: {required_data[key].shape}")
                print(f"  CombiConds shape: {required_data_old[key].shape}")
                # Check if shapes match but values differ
                if required_data[key].shape == required_data_old[key].shape:
                    diff = (~required_data[key].eq(required_data_old[key])).sum()
                    print(f"  Number of different values: {diff}")
        
        # Compare updated parameters
        print("\nComparing updated parameters:")
        if updated_params == updated_params_old:
            print("✅ Updated parameters are identical")
        else:
            print("❌ Updated parameters differ")
            # Compare first parameter as an example
            if updated_params and updated_params_old:
                print("First parameter comparison:")
                for key in set(updated_params[0].keys()):
                    if key in updated_params_old[0]:
                        if updated_params[0][key] == updated_params_old[0][key]:
                            print(f"  ✅ '{key}' matches")
                        else:
                            print(f"  ❌ '{key}' differs:")
                            print(f"    CombiConds2: {updated_params[0][key]}")
                            print(f"    CombiConds: {updated_params_old[0][key]}")

    compare_results(required_data, updated_params, required_data_old, updated_params_old)

def run_test():
    import json
    from tqdm import tqdm
    def load_strategies() -> dict:
        """
        Load trading strategies from JSON file.
        
        Returns:
            dict: Trading strategies configuration
        """
        path = "/home/ubuntu/Dang/strategies2.json"
        try:
            with open(path, 'r') as f:
                strategies = json.load(f)
            return strategies
        except Exception as e:
            print(f"Error loading strategies: {str(e)}")
            return {}
        
    strategies = load_strategies()

    group_conds = ['BidAskCS', 'BUSD', 'FBuySell' ]
    other_conds = ['F1', 'VN30', 'VNINDEX', 'BidAskF1', 'ArbitUnwind', 'PremiumDiscount']

    # Process each strategy sequentially with progress tracking
    with tqdm(total=len(strategies), desc="Processing signals") as pbar:
        for strategy in strategies:
            try:
                group = strategy['group']
                if group in group_conds:
                    test(strategy['conditions'], data_source='group')
                else:
                    test(strategy['conditions'], data_source='one_series')
                
            except Exception as e:
                print(f"Error processing signals for {strategy['name']}: {str(e)}")
            
            pbar.update(1)
