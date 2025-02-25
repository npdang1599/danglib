"""Import functions"""

import pprint
import logging
from danglib.pylabview.core_lib import pd, np, Ta, Utils, Adapters, Math, Fns
from numba import njit
from pymongo import MongoClient
import logging
import warnings
import argparse
from danglib.chatbots.viberbot import create_f5bot
from danglib.utils import show_ram_usage_mb

pd.options.mode.chained_assignment = None


# Tắt tất cả các FutureWarning
warnings.simplefilter(action='ignore', category=FutureWarning)

class CeleryTaskError(Exception):
    pass


class Globs:
    """Class of objects for global using purposes"""

    verbosity = 0
    data_from_day = "2017_01_01"

    def __init__(self):
        # self.df_vnindex: pd.DataFrame = Adapters.get_stocks_ohlcv_from_db_cafef(["VNINDEX"])
        self.df_vnindex: pd.DataFrame = None
        self.strategies_dic = {}
        self.strategies = []
        self.stocks, self.dic_groups = Adapters.get_stocks_beta_group()
        self.df_stocks = pd.DataFrame()
        self._df_stocks = None
        self.function_map = {
            "stock_scanner": Scanner.scan_multiple_stocks,
            "price_change": Conds.price_change,
            "price_change_vs_hl": Conds.price_change_vs_hl,
            "price_comp_ma": Conds.price_comp_ma,
            "price_gap": Conds.price_gap,
            "price_highest_lowest": Conds.PriceAction.price_highest_lowest,
            "consecutive_conditional_bars": Conds.PriceAction.consecutive_conditional_bars,
            "vol_comp_ma": Conds.vol_comp_ma,
            "vol_percentile": Conds.vol_percentile,
            "consecutive_squeezes": Conds.consecutive_squeezes,
            "ursi": Conds.ursi,
            "macd": Conds.macd,
            "bbwp": Conds.bbwp,
            "bbwp2": Conds.bbwp2,
            "bbpctb": Conds.bbpctb,
            "fourier_supertrend": Conds.fourier_supertrend,
            "mama" : Conds.mama,
            "net_income": Conds.Fa.net_income,
            "revenue": Conds.Fa.revenue,
            "inventory": Conds.Fa.inventory,
            "inventory_day": Conds.Fa.inventory_day,
            "capex_gross": Conds.Fa.capex_gross,
            "cash_debt_mktcap": Conds.Fa.cash_debt_mktcap,
            "PE": Conds.Fa.PE,
            "PB": Conds.Fa.PB,
            "index_cond": Conds.index_cond,
            "lookback_cond": Conds.lookback_cond,
            "steel_ma_and_hl": Conds.Sectors.steel_ma_and_hl,
            "iron_ore": Conds.Sectors.iron_ore,
            "china_hrc": Conds.Sectors.china_hrc,
            "coking_coal": Conds.Sectors.coking_coal,
            "china_rebar": Conds.Sectors.china_rebar,
            "steel_scrap": Conds.Sectors.steel_scrap,
            "hpg_margin": Conds.Sectors.hpg_margin,
            "brokerage_margin": Conds.Sectors.brokerage_margin,
            "brokerage_deposit_rate": Conds.Sectors.brokerage_deposit_rate,
            "vhc_us_asp": Conds.Sectors.vhc_us_asp,
            "raw_fish_price": Conds.Sectors.raw_fish_price,
            "us_price_spread": Conds.Sectors.us_price_spread,
            "north_hog_price": Conds.Sectors.north_hog_price,
            "south_hog_price": Conds.Sectors.south_hog_price,
            "hog_feed_cost": Conds.Sectors.hog_feed_cost,
            "hog_cash_spread": Conds.Sectors.hog_cash_spread,
            "china_p4_price": Conds.Sectors.china_p4_price,
            "china_p4_cash_spread": Conds.Sectors.china_p4_cash_spread,
            "dcm_ure": Conds.Sectors.dcm_ure,
            "dpm_ure": Conds.Sectors.dpm_ure,
            "ure_future": Conds.Sectors.ure_future,
            'wavetrend': Conds.wavetrend,
            'general_cond': Conds.general_cond
        }
        self.fa_funcs = [
            "net_income", 
            "revenue", 
            "inventory", 
            "inventory_day", 
            "capex_gross", 
            "cash_debt_mktcap", 
            "brokerage_margin", 
            "lookback_cond"
        ]
        self.sectors = {}
        self.df_all_index: pd.DataFrame = None
        self.index_names = ['VN30', 'VNDIAMOND', 'VNINDEX', 'VNMID', 'VNSML']
        
    # def __getattribute__(self, name):
    #     if name == "df_stocks":
    #         if(self._df_stocks is None):
    #             self.load_stocks_data()
    #         return self._df_stocks
    #     else:
    #         return super().__getattribute__(name)
    
    def load_stocks_data(self):
        """run to load stocks data"""
        try:
            self._df_stocks = Adapters.load_stocks_data_from_plasma()
        except ValueError as e:
            logging.error(f"`load_stocks_data` function error: {e}")


    def get_one_stock_data(self, stock):
        """Get one stock data from df_stocks"""
        df_stocks = Adapters.load_stocks_data_from_plasma()
        df: pd.DataFrame = df_stocks[df_stocks["stock"] == stock]
        df = df.dropna(axis=1, how='all').reset_index(drop=True)
        return df
    
    def load_vnindex(self):
        # self.df_vnindex: pd.DataFrame = Adapters.get_stock_from_vnstock(
        #     "VNINDEX", from_day=self.data_from_day
        # )
        self.df_vnindex: pd.DataFrame = self.get_one_stock_data("VNINDEX")

    def gen_stocks_data(self, fn=None, collection=None, send_viber=False):
        try:
            if fn is None:
                fn = Fns.pickle_stocks_data
            stocks = self.stocks
            Adapters.prepare_stocks_data(
                stocks, 
                fn=fn, 
                db_collection=collection,
                to_pickle=False,
                to_mongo=True, 
                to_plasma=True
            )

            df = Adapters.load_stocks_data_from_main_db()
            latest_day = df['day'].max()
            
            msg = f"Stocks data update for Pylabview has been completed! Latest day: {latest_day}"
        except Exception as e:
            msg = f"Failed to update data for Pylabview, error: {e}"
            logging.error(msg)

        if send_viber:
            F5bot = create_f5bot()
            F5bot.send_viber(msg)        

    @staticmethod
    def get_saved_params():
        db = MongoClient(host="localhost", port = 27022)["dang_scanned"]
        col = db['strategies']
        df = pd.DataFrame(list(col.find({}, {"_id":0, "name":1})))
        df.rename(columns={'name':'strategies'})
        return df
    
    @staticmethod
    def load_params(name):
        func_map_new = {
            "price_change_cond": "price_change",
            "price_ma_cond": "price_comp_ma",
            "price_gap_cond": "price_gap",
            "price_highest_lowest": "price_highest_lowest",
            "price_cons_bars": "consecutive_conditional_bars",
            "vol_comp_ma_cond": "vol_comp_ma",
            "vol_percentile_cond": "vol_percentile",
            "consecutive_squeezes_cond": "consecutive_squeezes",
            "ursi_cond": "ursi",
            "macd_cond": "macd",
            "bbwp_cond": "bbwp",
            "bbpctb_cond": "bbpctb",
            "net_income": "net_income",
        }
        
        db = MongoClient(host="localhost", port = 27022)["dang_scanned"]
        col = db['strategies']
        df_all = pd.DataFrame(list(col.find({}, {"_id":0})))
        df = pd.DataFrame(df_all[df_all['name'] == name]['configs'].values[0])
        params = {}
        
        df_scan = df[df['function'] == 'stock_scanner']
        scan_params = dict(zip(df_scan['param_name'], df_scan['value']))
        
        df_stock = df[df['master_name'] == 'stock_params']
        for func, dff in df_stock.groupby('function'):
            params[func_map_new[func]] = dict(zip(dff['param_name'], dff['value'])) 
        
        index_params = {'index_params':{}}
        df_index = df[df['master_name'] == 'index_params']
        for func, dff in df_index.groupby('function'):
            index_params['index_params'][func_map_new[func]] = dict(zip(dff['param_name'], dff['value'])) 
        params['index_cond'] = index_params
            
        lkbk_params = {'lookback_params': {}}
        df_lookbk = df[df['master_name'] == 'lookback_params']
        for func, dff in df_lookbk.groupby('function'):
            if func == 'compute lookback':
                lkbk_params['n_bars'] = dff['value'].values[0]
            else:
                lkbk_params['lookback_params'][func_map_new[func]] = dict(zip(dff['param_name'], dff['value'])) 
        params['lookback_cond'] = lkbk_params
        
        return scan_params, params

    def load_sectors_data(self):
        self.sectors = Adapters.load_sectors_data()

    def get_sectors_stocks(self):        
        self_stocks = self.stocks
        
        import ast
        
        db = MongoClient("localhost", 27022)["pylabview_db"]
        col = db['watchlist']
        df_raw = pd.DataFrame(col.find({},{'_id':0}))
        df_raw = df_raw[df_raw.index < 20]
        sectors = dict(zip(df_raw['watchlist_name'], df_raw['watchlist_params'].map(ast.literal_eval)))
        
        data_list = []
        for sector, stocks in sectors.items():
            for stock in stocks:
                if stock in self_stocks:
                    data_list.append({'stock': stock, 'sector': sector})

        df = pd.DataFrame(data_list)

        for stock in self_stocks:
            if stock not in df['stock'].tolist():
                data_list.append({'stock':stock, 'sector':'other'})

        df = pd.DataFrame(data_list)    
        df = df.drop_duplicates()
    
        return df
    
    @staticmethod
    def old_saved_adapters(params: dict):
        net_income_params = params.get('net_income')
        if net_income_params is not None:

            if 'use_shift' in net_income_params.keys():
                use_shift = net_income_params.pop('use_shift')
                n_shift = net_income_params.pop('n_shift')
                params['stock_scanner']['use_shift'] = use_shift
                params['stock_scanner']['n_shift'] = n_shift
        return params  
    
    def load_all_data(self):
        self.load_stocks_data()
        self.load_vnindex()
        self.load_sectors_data() 
        
    def load_all_data2(self):
        # self.load_vnindex()
        self.load_sectors_data() 
        
    def load_index_data(self):
        self.df_all_index = Adapters.get_index_data(self.data_from_day)


class Conds:
    """Custom condition funcitions"""

    class Standards:
        """Utility condition functions"""

        @staticmethod
        def two_line_pos(
            line1: pd.Series,
            line2: pd.Series,  
            direction: str = "crossover",
            equal: bool = False,
            use_flag: bool = False,
            *args, **kwargs
        ):
            """Two line position condition"""
            res = None
            if use_flag:
                if direction == "crossover":
                    res = Ta.crossover(line1, line2)
                elif direction == "crossunder":
                    res = Ta.crossunder(line1, line2)
                elif direction == "above":
                    res = line1 >= line2 if equal else line1 > line2
                elif direction == "below":
                    res = line1 <= line2 if equal else line1 < line2

            return res

        @staticmethod
        def range_cond(line, lower_thres, upper_thres, use_flag, *args, **kwargs):
            """Use flag wrapper or Utils.in_range function"""
            res = None
            if use_flag:
                res = Utils.in_range(line, lower_thres, upper_thres)

            return res

        @staticmethod
        def two_line_conditions(
            line1: pd.Series,
            line2: pd.Series,
            use_vs_signal: bool = False,
            direction: str = "crossover",
            use_range: bool = False,
            lower_thres: float = 0,
            upper_thres: float = 0,
            *args, **kwargs
        ):
            """Two line conditions"""
            pos_cond = Conds.Standards.two_line_pos(
                line1, line2, direction, use_flag=use_vs_signal
            )
            range_cond = Conds.Standards.range_cond(
                line1, lower_thres, upper_thres, use_flag=use_range
            )
            res = Utils.combine_conditions([pos_cond, range_cond])
            return res
        
        @staticmethod
        def two_ma_lines(
            df: pd.DataFrame,
            src_name: str,
            ma_len1=5,
            ma_len2=15,
            ma_type="EMA",
            ma_dir="above",
            use_flag: bool = True,
            *args, **kwargs
        ):
            """Check if the two moving averages (MA) of the price
            crossover, crossunder, are above, or below each other.

            Args:
                df (pd.DataFrame): ohlcv dataframe
                ma_len1 (int): MA1 length
                ma_len2 (int): MA2 length
                ma_type (str): "EMA", "SMA"
                ma_dir (str): "crossover", "crossunder", "above", "below"

            Returns:
                pd.Series[bool]: True or False if use_flag is True else None
            """
            if use_flag:
                src = df[src_name]
                ma1 = Ta.ma(src, ma_len1, ma_type)
                ma2 = Ta.ma(src, ma_len2, ma_type)

                if ma_dir == "crossover":
                    price_ma_cond = Ta.crossover(ma1, ma2)
                if ma_dir == "crossunder":
                    price_ma_cond = Ta.crossunder(ma1, ma2)
                if ma_dir == "above":
                    price_ma_cond = ma1 > ma2
                if ma_dir == "below":
                    price_ma_cond = ma1 < ma2

                return price_ma_cond

            return None
        
        @staticmethod
        def hlest_cond(df: pd.DataFrame, src_name: str, hl_options:str='highest', n_bars: int = 10, use_flag: bool = False, *args, **kwargs):
            if use_flag:
                src = df[src_name]
                func = Ta.is_highest if hl_options == 'highest' else Ta.is_lowest
                cond = func(src, n_bars)
                return cond

            return None
        
        @staticmethod
        def std_cond(df: pd.DataFrame, src_name: str, n_bars: int = 10, mult: float = 2.0, position:str = 'higher', use_flag:bool = False, *args, **kwargs):
            if use_flag:
                src = df[src_name]
                std = Ta.stdev(src, n_bars)
                mean = Ta.sma(src, n_bars)
                upper = mean + std * mult
                cond = src > upper if position == 'higher' else src < upper
                return cond
            
            return None

    @staticmethod
    def is_in_top_bot_percentile(
        df: pd.DataFrame, 
        src_name: str = "close",
        lookback_period: int = 20,
        direction: str = 'top',
        percentile_threshold: float = 90,
        use_flag: bool = False,
        *args, **kwargs
    ):
        if use_flag:
            src = df[src_name]
            rank = Ta.rolling_rank(src, lookback_period)

            thres = percentile_threshold if direction == 'top' else 100 - percentile_threshold
            cond = rank >= thres if direction == 'top' else rank <= thres

            return cond

        return None

    @staticmethod
    def consecutive_above_below(
        line1: pd.Series,
        line2: pd.Series,
        direction: str = "above",
        num_bars: int = 5,
        use_flag: bool = False,
        *args, **kwargs
    ):
        """Check if line1 is continuously above/below line2 for X bars
        
        Args:
            df (pd.DataFrame): Input dataframe
            line1_name (str): Name of first line/source in dataframe
            line2_name (str): Name of second line/source in dataframe
            direction (str): "above" or "below"
            num_bars (int): Number of consecutive bars needed 
            use_flag (bool): Whether to perform calculation
            
        Returns:
            pd.Series: Boolean series where True means condition is met
        """
        if use_flag:
            # Calculate condition for each bar
            condition = line1 > line2 if direction == "above" else line1 < line2
            
            # Count consecutive occurrences 
            consecutive_count = condition.astype(int).rolling(window=num_bars).sum()
            
            # True if we have num_bars consecutive occurrences
            signal = consecutive_count >= num_bars
            
            return signal
            
        return None

    @staticmethod
    def cross_no_reverse(
        line1: pd.Series,
        line2: pd.Series,
        direction: str = "crossover", 
        bars_no_reverse: int = 5,
        use_flag: bool = False,
        *args, **kwargs
    ):
        """Check if line1 crosses over/under line2 and there is no reverse cross signal
        within the next X bars, using vectorized operations.

        Args:
            df (pd.DataFrame): Input dataframe
            src1_name (str): Name of first line/source in dataframe
            src2_name (str): Name of second line/source in dataframe
            direction (str): "crossover" or "crossunder"  
            bars_no_reverse (int): Number of bars to check for no reverse signal
            use_flag (bool): Whether to perform calculation

        Returns:
            pd.Series: Boolean series where True means condition is met
        """
        def test():
            ursi, sig = Ta.ursi(df_raw['close'])
            line1 = ursi
            line2 = sig
            direction: str = "crossover" 
            bars_no_reverse: int = 5
            use_flag: bool = False

        if use_flag:
            
            # Get initial cross signals
            if direction == "crossover":
                cross_signals = Ta.crossover(line1, line2)
                reverse_signals = Ta.crossunder(line1, line2)
            else:  # crossunder
                cross_signals = Ta.crossunder(line1, line2)
                reverse_signals = Ta.crossover(line1, line2)

            # Create a rolling window view of reverse signals
            # For each point, look at next X bars for reverse signals
            reverse_windows = reverse_signals.rolling(
                window=bars_no_reverse,
                min_periods=1
            ).sum()

            # Valid signals are cross points where there are no reverse signals 
            # in the next X bars
            valid_signals = cross_signals.shift(bars_no_reverse) & (reverse_windows == 0)

            return valid_signals.fillna(False)

        return None

    @staticmethod
    def price_change(
        df: pd.DataFrame,
        src_name: str = "close",
        periods: int = 1,
        direction: str = "increase",
        lower_thres: float = 0,
        upper_thres: float = 100,
        use_flag: bool = True,
        *args, **kwargs
    ):
        """Check if the percentage change in price over a period of time
        falls within the specified range.

        Args:
            df (pd.DataFrame): ohlcv dataframe
            periods (int): period
            direction (str): "increase" or "decrease"
            lower_thres (float): lower range threshold
            upper_thres (float): upper range threshold

        Returns:
            pd.Series[bool]: True or False if use_flag is True else None
        """
        src = df[src_name]
        if use_flag:
            pct_change = src.pct_change(periods=periods).round(6) * 100
            if direction == "decrease":
                pct_change = pct_change * -1

            return Utils.in_range(pct_change, lower_thres, upper_thres, equal=False)

        return None

    @staticmethod
    def price_comp_ma(
        df: pd.DataFrame,
        ma_len1=5,
        ma_len2=15,
        ma_type="EMA",
        ma_dir="above",
        use_flag: bool = True,
        *args, **kwargs
    ):
        """Check if the two moving averages (MA) of the price
        crossover, crossunder, are above, or below each other.

        Args:
            df (pd.DataFrame): ohlcv dataframe
            ma_len1 (int): MA1 length
            ma_len2 (int): MA2 length
            ma_type (str): "EMA", "SMA"
            ma_dir (str): "crossover", "crossunder", "above", "below"

        Returns:
             pd.Series[bool]: True or False if use_flag is True else None
        """
        if use_flag:
            src = df["close"]
            ma1 = Ta.ma(src, ma_len1, ma_type)
            ma2 = Ta.ma(src, ma_len2, ma_type)

            if ma_dir == "crossover":
                price_ma_cond = Ta.crossover(ma1, ma2)
            if ma_dir == "crossunder":
                price_ma_cond = Ta.crossunder(ma1, ma2)
            if ma_dir == "above":
                price_ma_cond = ma1 > ma2
            if ma_dir == "below":
                price_ma_cond = ma1 < ma2

            return price_ma_cond

        return None
    
    @staticmethod
    def price_change_vs_hl(
        df: pd.DataFrame,
        src_name = 'close',
        use_flag: bool = False, 
        direction: str = "Increase",
        nbars: int = 10,
        low_range: float = 5,
        high_range: float = 100,
        wait_bars: bool = False,
        wbars: int = 5,
        compare_prev_peak_trough: bool = False,
        compare_prev_peak_trough_direction: str = "Higher",
        *args, **kwargs
        ):
        """Check if the percentage change between `close` price with lowest(`low`) (if direction is increase) 
        or highest(`high`)(decrease) over a period of time falls within the specified range.

        Args:
            df (pd.DataFrame): ohlcv dataframe
            
            use_flag (bool, optional): if use_flag set to False return None, 
            calculating condition otherwise. Defaults to False.
            
            direction (str, optional): `Increase` or `Decrease`. Defaults to "Increase".
            
            nbars (int, optional): periods. Defaults to 10.
            
            low_range (float, optional): low threshold. Defaults to 5.
            
            high_range (float, optional): high threshold. Defaults to 100.

        Returns:
            pd.Series[bool]: True or False if use_flag is True else None
        """
        

        
        if use_flag:
            close = df[src_name]
            comp_src = Ta.lowest(df['low'], nbars) if direction == 'Increase' else Ta.highest(df['high'], nbars)
            pct_change = Utils.calc_percentage_change(comp_src, close)
            if direction == "Decrease":
                pct_change = pct_change * -1

            if wait_bars == False:
                return Utils.in_range(pct_change, low_range, high_range, equal=True)

            inrange = Utils.in_range(pct_change, low_range, high_range, equal=True)
            if direction == "Increase":
                mark = np.where(inrange, df['high'], np.nan)
                mark = pd.Series(mark, index=df.index)
                hlofclose = Ta.highest(df['close'], wbars)
            else:
                mark = np.where(inrange, df['low'], np.nan)
                mark = pd.Series(mark, index=df.index)
                hlofclose = Ta.lowest(df['close'], wbars)
                
            fwmark = mark.shift(wbars)
            rs = Utils.new_1val_series(False, df) 
            if direction == "Increase":
                isw = hlofclose < fwmark
            else:
                isw = hlofclose > fwmark
            rs.loc[(fwmark.notna() & isw)] = True

            if not compare_prev_peak_trough:
                return rs
            else:
                df['res'] = rs
                df['res'] = df['res'].astype(bool)
                df['cons_res'] = df['res'].groupby((df['res'] != df['res'].shift(1)).cumsum()).cumsum()
                df["group"] = (df["res"] != df["res"].shift()).cumsum()

                if direction == 'Increase':
                    df['peak'] = np.where(df['res'], df['high'].rolling(wbars + nbars).max(), np.nan)
                    df['peak'] = df['peak'].groupby((df['res'] != df['res'].shift(1)).cumsum()).cummax()
                    grouped = df[df['res']].groupby((df['res'] != df['res'].shift(1)).cumsum()).agg({'peak':'max'})
                    grouped_shifted = grouped.shift()
                    df = df.merge(grouped_shifted, left_on="group", right_index = True, suffixes=("", "_prev"), how = 'left')
                    if compare_prev_peak_trough_direction == 'Higher':
                        df['new_res'] = df['peak'] > df['peak_prev']
                    else:
                        df['new_res'] = df['peak'] < df['peak_prev']
                if direction == 'Decrease':
                    df['trough'] = np.where(df['res'], df['low'].rolling(wbars + nbars).min(), np.nan)
                    df['trough'] = df['trough'].groupby((df['res'] != df['res'].shift(1)).cumsum()).cummin()
                    grouped = df[df['res']].groupby((df['res'] != df['res'].shift(1)).cumsum()).agg({'trough':'min'})
                    grouped_shifted = grouped.shift()
                    df = df.merge(grouped_shifted, left_on="group", right_index = True, suffixes=("", "_prev"), how = 'left')
                    if compare_prev_peak_trough_direction == 'Higher':
                        df['new_res'] = df['trough'] > df['trough_prev']
                    else:
                        df['new_res'] = df['trough'] < df['trough_prev']
                return df['new_res']  
        return None
    @staticmethod
    def no_break(
        df: pd.DataFrame,
        direction: str = "Increase",
        wbars: int = 5,
        use_flag: bool = False,
    ):
        inrange = Utils.new_1val_series(True, df)
        if use_flag:
            if direction == "Increase":
                mark = np.where(inrange, df['high'], np.nan)
                mark = pd.Series(mark, index=df.index)
                hlofclose = Ta.highest(df['close'], wbars)
            else:
                mark = np.where(inrange, df['low'], np.nan)
                mark = pd.Series(mark, index=df.index)
                hlofclose = Ta.lowest(df['close'], wbars)
                
            fwmark = mark.shift(wbars)
            rs = Utils.new_1val_series(False, df) 
            if direction == "Increase":
                isw = hlofclose < fwmark
            else:
                isw = hlofclose > fwmark
            rs.loc[(fwmark.notna() & isw)] = True
    @staticmethod
    def price_gap(df: pd.DataFrame, gap_dir="Use Gap Up", use_flag: bool = True, *args, **kwargs):
        """Check if this is Gap Up or Gap Down bar.

        Args:
            df (pd.DataFrame): ohlcv dataframe
            gap_dir (_type_): "Use Gap Up" or "Use Gap Down"

        Returns:
            pd.Series[bool]: True or False
        """

        if use_flag:
            p_o = df["open"]
            p_h = df["high"]
            p_l = df["low"]
            p_c = df["close"]

            assert gap_dir in [
                "Use Gap Up",
                "Use Gap Down",
            ], "`gap_dir` must be in ['Use Gap Up', 'Use Gap Down']"

            if gap_dir == "Use Gap Up":
                cond = (p_o > p_c.shift(1)) & (p_l > p_h.shift(1))
            elif gap_dir == "Use Gap Down":
                cond = (p_o < p_c.shift(1)) & (p_h < p_l.shift(1))

            return cond

        return None

    @staticmethod
    def vol_comp_ma(
        df: pd.DataFrame,
        v_type="volume",
        n_bars=1,
        ma_len=20,
        comp_ma_dir="higher",
        comp_ma_perc=20,
        use_flag: bool = True, 
        *args, **kwargs
    ):
        """Compare volume(value) with MA

        Args:
            df (pd.DataFrame): ohlcv dataframe
            v_type (_type_): "volume" or "value"
            n_bars (_type_): period
            ma_len (_type_): volume smooth length
            comp_ma_dir (_type_): "higher" or "lower"
            comp_ma_perc (_type_): percentage different

        Returns:
            pd.Series[bool]: True or False
        """
        if use_flag:
            v = df[v_type]

            comp_v = Ta.sma(v, n_bars)
            vol_ma = Ta.sma(v, ma_len)
            vcp = Utils.calc_percentage_change(vol_ma, comp_v)
            vcp *= 1 if comp_ma_dir == "higher" else -1
            avcp = Ta.sma(vcp, n_bars)

            return avcp >= comp_ma_perc

        return None

    class PriceAction:
        """Price Action session conditions"""

        @staticmethod
        def price_highest_lowest(
            df: pd.DataFrame,
            method: str = "Highest",
            num_bars: int = 10,
            use_flag: bool = True, 
            *args, **kwargs
        ):
            """Check if close price is highest or lowest over the latest n bars

            Args:
                df (pd.DataFrame): ohlcv datafrane
                method (str): "Highest" or "Lowest"
                num_bars (int):

            Returns:
                pd.Series: True or False
            """
            assert method in [
                "Highest",
                "Lowest",
            ], "`method` must be `Highest` or `Lowest`"

            if use_flag:
                p_c = df["close"]

                return (
                    Ta.is_highest(p_c, num_bars)
                    if method == "Highest"
                    else Ta.is_lowest(p_c, num_bars)
                )

            return None

        @staticmethod
        def consecutive_conditional_bars(
            df: pd.DataFrame,
            src1_name: str,
            src2_name: str,
            direction: str,
            num_bars: int,
            num_matched: int,
            use_flag: bool = True, 
            *args, **kwargs
        ):
            """Check if there are enough increasing (decreasing) bars within the last N bars.
            It is possible to check two sources simultaneously.

            Args:
                df (pd.DataFrame): ohlcv dataframe
                src1_name (str): source 1 (open, high, low, close)
                src2_name (str): source 2 (open, high, low, close)
                direction (str): "Increase" or "Decrease"
                num_bars (int): The number of the latest bars that need to be checked
                num_matched (int): The number of the latest bars that need to meet the condition

            Returns:
                pd.Series: True or False
            """
            if use_flag:
                src1 = df[src1_name]
                src2 = df[src2_name]

                matched1 = Utils.count_changed(src1, num_bars, direction) >= num_matched
                matched2 = Utils.count_changed(src2, num_bars, direction) >= num_matched

                return matched1 & matched2

            return None

    @staticmethod
    def vol_percentile(
        df: pd.DataFrame,
        ma_length: int = 10,
        ranking_window: int = 128,
        low_range: float = 0,
        high_range: float = 100,
        use_flag: bool = True, 
        *args, **kwargs
    ):
        """Check if percentile of volume(value) is within the customed range

        Args:
            df (pd.DataFrame): ohlcv dataframe
            ma_length (int, optional): Smooth length. Defaults to 10.
            ranking_window (int, optional): Length of the ranking window. Defaults to 128.
            low_range (float, optional): low threshold. Defaults to 0.
            high_range (float, optional): high threshold. Defaults to 100.
            use_flag (bool, optional): Perform calculation when use_flag is set to True
            else return a Series of True values. Defaults to True.

        Returns:
            pd.Series: True or False
        """
        v = df["volume"]
        if use_flag:
            smoothed_v = Ta.sma(v, ma_length)
            smoothed_v_rank = (smoothed_v.rolling(ranking_window).rank() - 1) / (ranking_window - 1) * 100
            return Utils.in_range(smoothed_v_rank, low_range, high_range)

        return None

    @staticmethod
    def consecutive_squeezes(
        df: pd.DataFrame,
        src_name: str = "close",
        bb_length: int = 20,
        length_kc: int = 20,
        mult_kc: float = 1.5,
        use_true_range: bool = True,
        num_bars: int = 1,
        use_no_sqz:bool =  False,
        use_flag: bool = False, 
        *args, **kwargs
    ):
        """Check if squeeze occurs continuously within the last n bars.

        Args:
            df (pd.DataFrame): ohlcv dataframe
            src_name (str, optional): source (open, high, low, close). Defaults to 'close'.
            bb_length (int, optional): BB length. Defaults to 20.
            length_kc (int, optional): KC length. Defaults to 20.
            mult_kc (float, optional): KC mult factor. Defaults to 1.5.
            use_true_range (bool, optional): Use true range (KC) or not. Defaults to True.
            num_bars (int, optional): The minimum number squeeze bars. Defaults to 1.
            use_flag (bool, optional): Perform calculation when use_flag is set to True
            else return a Series of True values. Defaults to True.

        Returns:
            pd.Series: True or False
        """
        if use_flag:
            bb_length = int(bb_length)
            length_kc = int(length_kc)
            num_bars = int(num_bars)
            # Calculating squeeze
            sqz_on, sqz_off, no_sqz = Ta.squeeze(
                df, src_name, bb_length, length_kc, mult_kc, use_true_range
            )

            # # Count consecutive squeeze on
            # cons_sqz_num = Utils.count_consecutive(sqz_on)
            # return cons_sqz_num >= num_bars
        
            if use_no_sqz:
                return sqz_off
            else:
                # Count consecutive squeeze on
                cons_sqz_num = Utils.count_consecutive(sqz_on)
                return cons_sqz_num >= num_bars

        return None

    @staticmethod
    def ursi(
        df: pd.DataFrame,
        length: int = 14,
        smo_type1: str = "RMA",
        smooth: int = 14,
        smo_type2: str = "EMA",
        use_vs_signal: bool = False,
        direction: str = "crossover",
        use_range: bool = False,
        lower_thres: float = 0,
        upper_thres: float = 0,
        use_flag: bool = False, 
        *args, **kwargs
    ):
        """Conditions base on URSI and URSI Signal

        Args:
            df (pd.DataFrame): ohlcv DataFrame.

            ursi_setting_params (dict, optional): URSI setting params.

            example: { "length": 14, "smo_type1": "RMA", "smooth": 14, "smo_type2": "EMA" }.

            use_vs_signal (bool, optional): Check to use the condition based on the position
            between the URSI line and the signal line. Defaults to False.

            direction (str, optional): above, below, crossover, crossunder. Defaults to "crossover".

            use_range (bool, optional): Check to use the condition based on URSI line range.
            Condition met if URSI is within an input range.
            Defaults to False.

            lower_thres (float, optional): lower range. Defaults to 0.
            upper_thres (float, optional): upper range. Defaults to 0.

            use_flag (bool, optional): Perform calculation when use_flag is set to True
            else return a Series of True values. Defaults to True.

        Returns:
            pd.Series: True or False
        """
        src = df["close"]

        res = None
        if use_flag:
            ursi, signal = Ta.ursi(
                src, 
                length,
                smo_type1,
                smooth,
                smo_type2,
            )

            res = Conds.Standards.two_line_conditions(
                ursi,
                signal,
                use_vs_signal,
                direction,
                use_range,
                lower_thres,
                upper_thres,
            )

        return res

    @staticmethod
    def macd(
        df: pd.DataFrame,
        src_name: str = "close",
        r2_period: int = 20,
        fast: int = 10,
        slow: int = 20,
        signal_length: int = 9,
        use_vs_signal: bool = False,
        direction: str = "crossover",
        use_range: bool = False,
        lower_thres: float = 0,
        upper_thres: float = 0,
        use_flag: bool = False, 
        *args, **kwargs
    ):
        """Conditions base on MACD and MACD Signal

        Args:
            src (pd.Series): input source (open, high, low, close)

            macd_setting_params (dict, optional): MACD setting params.
            if this param is None, fuction will use the following defaul setting:
            {
                "src_name": "close",
                "r2_period": 20,
                "fast": 10,
                "slow": 20,
                "signal_length": 9
            }

            use_vs_signal (bool, optional): Check to use the condition based on the position
            between the MACD line and the signal line. Defaults to False.

            direction (str, optional): above, below, crossover, crossunder. Defaults to "crossover".

            use_range (bool, optional): Check to use the condition based on MACD line range.
            Condition met if MACD is within an input range.
            Defaults to False.

            lower_thres (float, optional): lower range. Defaults to 0.
            upper_thres (float, optional): upper range. Defaults to 0.

            use_flag (bool, optional): Perform calculation when use_flag is set to True
            else return a Series of True values. Defaults to True.

        Returns:
            pd.Series: True or False
        """
        res = None
        if use_flag:
            macd, signal = Ta.macd(
                df,
                src_name,
                r2_period,
                fast,
                slow,
                signal_length,
            )

            res = Conds.Standards.two_line_conditions(
                macd,
                signal,
                use_vs_signal,
                direction,
                use_range,
                lower_thres,
                upper_thres,
            )
            


        return res

    @staticmethod
    def bbwp(
        df: pd.DataFrame,
        src_name: str = "close",
        basic_type: str = "SMA",
        bbwp_len: int = 13,
        bbwp_lkbk: int = 128,
        use_low_thres: bool = False,
        low_thres: float = 20,
        use_high_thres: bool = False,
        high_thres: float = 80,
        use_flag: bool = False, 
        *args, **kwargs
    ):
        """bbwp based conditions"""

        res = None
        if use_flag:
            bbwp = Ta.bbwp(df, src_name, basic_type, bbwp_len, bbwp_lkbk)

            if use_low_thres:
                res = bbwp <= low_thres

            if use_high_thres:
                res = bbwp >= high_thres

        return res

    @staticmethod
    def bbwp2(
        df: pd.DataFrame,
        src_name: str = "close",
        basic_type: str = "SMA",
        bbwp_len: int = 13,
        bbwp_lkbk: int = 128,
        low_thres: float = 20,
        high_thres: float = 80,
        use_flag: bool = False, 
        *args, **kwargs
    ):
        """bbwp based conditions"""

        res = None
        if use_flag:
            bbwp = Ta.bbwp(df, src_name, basic_type, bbwp_len, bbwp_lkbk)
            res = Utils.in_range(bbwp, low_thres, high_thres)
        return res




    @staticmethod
    def bbpctb(
        df: pd.DataFrame,
        src_name: str = "close",
        length: int = 20,
        mult: float = 2,
        use_range: bool = False,
        low_range: float = 80,
        high_range: float = 100,
        use_cross: bool = False,
        direction: str = "crossover",
        cross_line: str = "Upper band",
        use_flag: bool = False, 
        *args, **kwargs
    ):
        """bbpctb based conditions"""
        res = None

        if use_flag:
            bbpctb = Ta.bbpctb(df, src_name, length, mult)

            cross_l = Utils.new_1val_series(100 if cross_line == "Upper band" else 0, df)

            res = Conds.Standards.two_line_conditions(
                bbpctb,
                cross_l,
                use_cross,
                direction,
                use_range,
                low_range,
                high_range,
            )

        return res

    @staticmethod
    def wavetrend(
        df: pd.DataFrame, 
        use_flag: bool = False,
        direction: str = 'crossover',

        
        ):
        # WT_n1 = 10
        # WT_n2 = 21

        # WT_ap = hlc3 
        # WT_esa = f_ma(WT_ap, WT_n1, 'EMA') // esa = ema(ap, n1)
        # WT_d = f_ma(math.abs(WT_ap - WT_esa), WT_n1, 'EMA')
        # WT_ci = (WT_ap - WT_esa) / (0.015 * WT_d)
        # WT_tci = f_ma(WT_ci, WT_n2, 'EMA')
        
        # WT_wt1 = WT_tci
        # WT_wt2 = f_ma(WT_wt1, 4, 'SMA')

        # WT_Over = ta.crossover(WT_wt1, WT_wt2)
        # WT_Under = ta.crossunder(WT_wt1, WT_wt2)

        # WT1AboveWT2 = WT_wt1 > WT_wt2
        # WT1BelowWT2 = WT_wt1 < WT_wt2

        # C21 = switch
        #     WT1AboveWT2 and not WT_Over => 'Above'
        #     WT1BelowWT2 and not WT_Under => 'Below'
        #     WT_Over => 'Crossover'
        #     WT_Under => 'Crossunder'
        res = None

        if use_flag:
            wt1, wt2 = Ta.wavetrend(df)
            res = Conds.Standards.two_line_pos(wt1, wt2, direction = direction, use_flag = use_flag)
        
        return res
    @staticmethod
    def fourier_supertrend(
        df: pd.DataFrame,
        fft_period: int = 14,
        fft_smooth: int = 7,
        harmonic_weight: float = 0.5,
        vol_length: int = 10,
        vol_mult: float = 2.0,
        vol_smooth: int = 10,
        use_range: bool = False,
        low_range: float = 80,
        high_range: float = 100,
        use_cross: bool = False,
        direction: str = "crossover",
        cross_line: float = 50,
        use_flag: bool = False, 
    ):
        res = None

        if use_flag:

            fft_trend, upper_band, lower_band  = Ta.fourier_supertrend(
                df,
                fft_period,
                fft_smooth,
                harmonic_weight,
                vol_length,
                vol_mult,
                vol_smooth,
            )
            fft_trend_ratio = (df['close'] - lower_band) / (upper_band - lower_band) * 100

            cross_l = Utils.new_1val_series(cross_line, df)

            res = Conds.Standards.two_line_conditions(
                fft_trend_ratio,
                cross_l,
                use_cross,
                direction,
                use_range,
                low_range,
                high_range,
            )

        return res
    @staticmethod
    def mama(
        df: pd.DataFrame,
        fastlimit=0.5,
        slowlimit=0.05,
        use_cross: bool = False,
        direction: str = "crossover",
        use_flag: bool = False, 
    ):
        res = None

        if use_flag:

            mama, fama = Ta.mama(df, fastlimit, slowlimit)
            res = Conds.Standards.two_line_pos(mama, fama, direction = direction, use_flag = use_flag)
        return res
    @staticmethod
    def general_cond(
                    df: pd.DataFrame,
                    src_name: str = 'close',
                    pct_change_use_flag: bool = False,
                    pct_change_n_bar: int = 1,
                    pct_change_lower_thres: int = 0,
                    pct_change_upper_thres: int = 10,
                    ma_use_flag: bool = False,
                    ma_type: str = 'EMA',
                    ma_len1: int = 1,
                    ma_len2: int = 15,
                    ma_pos: str = 'above',
                    range_use_flag: bool = False,
                    range_lower_thres: float = 0,
                    range_upper_thres: float = 100,
                    hline_use_flag: bool = False,
                    hline_thres: float = 0,
                    hline_pos: str = 'above',
                    pct_use_flag: bool = False,
                    pct_ranking_window: int = 128,
                    pct_lower_thres: float = 0,
                    pct_upper_thres: float = 100,
                    no_cross_use_flag: bool = False,
                    no_cross_pos: str = 'crossover',
                    # no_cross_matype: str = 'EMA',
                    # no_cross_malen1: int =1,
                    # no_cross_malen2: int = 15,
                    no_break_use_flag: bool = False,
                    no_break_mark: str = 'high',
                    no_break_compare_value: str = 'close',
                    no_break_compare_direction: str = 'higher',
                    change_use_flag: bool = False,
                    change_mark: str = 'close',
                    change_lower_thres: float = 0,
                    change_upper_thres: float = 100,
                    wait_bars: int = 5,
                    lookback_use_flag: bool = False,
                    lookback_n_bars: int = 5
                    ):
        series = df[src_name]
        
        pct_change_value = series.pct_change(periods = pct_change_n_bar) * 100
        pct_change_cond = Conds.Standards.range_cond(pct_change_value, 
                                                     lower_thres= pct_change_lower_thres,
                                                     upper_thres= pct_change_upper_thres,
                                                     use_flag = pct_change_use_flag)
        
        ma1 = Ta.ma(series, length = ma_len1, ma_type= ma_type)
        ma2 = Ta.ma(series, length = ma_len2, ma_type= ma_type)
        ma_cond = Conds.Standards.two_line_pos(line1 = ma1, line2 = ma2, direction= ma_pos, use_flag= ma_use_flag)

        range_cond = Conds.Standards.range_cond(series, lower_thres= range_lower_thres, upper_thres= range_upper_thres, use_flag= range_use_flag)
        
        hline = Utils.new_1val_series(hline_thres, series_to_copy_index= series)
        hline_cond = Conds.Standards.two_line_pos(line1 = series, line2 = hline, direction= hline_pos, use_flag= hline_use_flag)
        
        pct_series = Ta.rolling_rank(series, ranking_window= pct_ranking_window)
        pct_cond = Conds.Standards.range_cond(pct_series, lower_thres= pct_lower_thres, upper_thres= pct_upper_thres, use_flag= pct_use_flag)
        
        cond = Utils.combine_conditions([pct_change_cond, ma_cond, range_cond, hline_cond, pct_cond])
        if cond is None:
            cond = Utils.new_1val_series(True, series_to_copy_index= series)
            
        if no_cross_use_flag:
            no_cross_cfm_cond = Conds.Standards.two_line_pos(line1 = ma1, line2 = ma2, direction= no_cross_pos, use_flag= True)
            no_cross_cfm_cond_rolling = (no_cross_cfm_cond.rolling(wait_bars).sum() == 0)
            cond = cond.shift(wait_bars) & no_cross_cfm_cond_rolling
        
        if no_break_use_flag:
            if no_break_mark == 'threshold':
                no_break_mark_src = pd.Series(hline_thres, index = series.index)    
            else:
                no_break_mark_src = np.where(cond, df[no_break_mark], np.nan) 
                no_break_mark_src = pd.Series(no_break_mark_src, index = series.index)
            
            no_break_mark_src = no_break_mark_src.ffill()
            no_break_compare_src = df[no_break_compare_value]
            no_break_cfm_cond = no_break_compare_src < no_break_mark_src if no_break_compare_direction == 'higher' else no_break_compare_src > no_break_mark_src
            no_break_cfm_cond_rolling = no_break_cfm_cond.rolling(wait_bars).sum() == 0
            cond = cond.shift(wait_bars) & no_break_cfm_cond_rolling
        
        if change_use_flag:
            change_mark = np.where(cond, series, np.nan)
            change_mark = pd.Series(change_mark, index=df.index)
            
            non_nan_marks = change_mark.notna()
            within_5_rows = non_nan_marks.rolling(window=wait_bars, min_periods=1).sum().shift(1).fillna(0) > 0
            change_mark_filled = change_mark.ffill()
            percent_change = np.where(
                                within_5_rows,
                                (series - change_mark_filled) / change_mark_filled * 100,
                                np.nan
                                )
            percent_change = pd.Series(percent_change, index = df.index)
            
            cond = Conds.Standards.range_cond(
                percent_change, change_lower_thres, change_upper_thres, use_flag=change_use_flag
            )
        if lookback_use_flag:
            cond = cond.rolling(lookback_n_bars, closed = 'left').max().fillna(False).astype(bool)

        return cond

    class Fa:
        """FA conditions"""
        
        @staticmethod
        def growth2(
            df: pd.DataFrame,
            src_name: str,
            calc_type: str = "QoQ",
            roll_len: int = 1,
            direction: str = "positive",
            percentage: float = 0,
            use_flag: bool = False, 
            *args, **kwargs
        ):
            if use_flag:
                df["rollSum"] = Math.sum(df[src_name], roll_len)

                pct_change_periods = 1 if calc_type == "QoQ" else 4

                df["pctChange"] = (
                    Math.pct_change(df["rollSum"], pct_change_periods) * 100
                ).round(6)

                df["matched"] = (
                    df["pctChange"] > percentage
                    if direction == "positive"
                    else df["pctChange"] < -percentage
                )
                
                df["matched"] = np.where(
                    (np.isinf(df["pctChange"])) | (df['rollSum'] == 0),
                    False,
                    df["matched"]
                )
                
                return  df["matched"]
            
            return None

        @staticmethod
        def net_income(
            df: pd.DataFrame,
            calc_type: str = "QoQ",
            roll_len: int = 1,
            direction: str = "positive",
            percentage: float = 0,
            use_flag: bool = False,
            
            trend_calc_type: str= "QoQ",
            trend_rolling_len: int = 1 ,
            trend_direction: str = "increase",
            trend_n_quarters: float = 3,
            trend_growth: str = 'acceleration', # deceleration
            trend_use_flag: bool = False,
            
            use_shift = False,
            n_shift = 15, 
            *args, **kwargs
        ):
            """Net Income based conditions"""
            
            df_ni = df.groupby("mapYQ")[["netIncome"]].max()

            growth_cond = Conds.Fa.growth2(
                df_ni,
                src_name='netIncome',
                calc_type=calc_type,
                roll_len=roll_len,
                direction=direction,
                percentage=percentage,
                use_flag=use_flag
            )
            
            trend_cond = Conds.Fa.trend(
                df_ni,
                src_name='netIncome', 
                calc_type=trend_calc_type,
                rolling_len=trend_rolling_len,
                direction=trend_direction,
                number_of_quarters=trend_n_quarters,
                growth=trend_growth,
                use_flag=trend_use_flag
            )
            
            matched = Utils.combine_conditions([trend_cond, growth_cond])
            
            if matched is not None:
                df_ni['cond'] = matched
                df['cond'] = df['mapYQ'].map(df_ni['cond'])
                
                cond = df['cond']
                if use_shift:
                    cond = cond.shift(n_shift)
                    cond = cond.fillna(value=False)
                
                return cond

            return None
        
        @staticmethod
        def revenue(
            df: pd.DataFrame,
            calc_type: str = "QoQ",
            roll_len: int = 1,
            direction: str = "positive",
            percentage: float = 0,
            use_flag: bool = False,
            
            trend_calc_type: str= "QoQ",
            trend_rolling_len: int = 1 ,
            trend_direction: str = "increase",
            trend_n_quarters: float = 3,
            trend_growth: str = 'acceleration', # deceleration
            trend_use_flag: bool = True,

            use_shift = False,
            n_shift = 15, 
            *args, **kwargs
        ):
            """Revenue based conditions"""

            df_rev = df.groupby("mapYQ")[["revenue"]].max()
            
            growth_cond = Conds.Fa.growth2(
                df_rev,
                src_name='revenue',
                calc_type=calc_type,
                roll_len=roll_len,
                direction=direction,
                percentage=percentage,
                use_flag=use_flag
            )
            
            trend_cond = Conds.Fa.trend(
                df_rev,
                src_name='revenue', 
                calc_type=trend_calc_type,
                rolling_len=trend_rolling_len,
                direction=trend_direction,
                number_of_quarters=trend_n_quarters,
                growth=trend_growth,
                use_flag=trend_use_flag
            )
            
            matched = Utils.combine_conditions([trend_cond, growth_cond])
            
            if matched is not None:
                df_rev['cond'] = matched
                df['cond'] = df['mapYQ'].map(df_rev['cond'])
                cond = df['cond']
                
                if use_shift:
                    cond = cond.shift(n_shift)
                    cond = cond.fillna(value=False)
                
                return cond
            
            return None

        @staticmethod
        def PE(
            df: pd.DataFrame,
            use_flag = False,
            lowrange = 0,
            highrange = 10, 
            *args, **kwargs
        ):
            return Conds.Standards.range_cond(df['PE'], lower_thres=lowrange, upper_thres=highrange, use_flag=use_flag)
        
        @staticmethod
        def PB(
            df: pd.DataFrame,
            range_use_flag = False,
            range_low = 0,
            range_high = 10,
            
            bb_length: int = 20,
            bb_mult: float = 2.0,
            bb_upper_use_flag: bool = False,
            bb_upper_method: str = 'above', 
            bb_lower_use_flag: bool = False,
            bb_lower_method: str = 'below', 
            *args, **kwargs
        ):
            src = df['PB']
            
            range_cond = Conds.Standards.range_cond(
                src, 
                lower_thres=range_low, 
                upper_thres=range_high, 
                use_flag=range_use_flag
            )
            
            basis = Ta.sma(src, bb_length)
            dev = bb_mult * Ta.stdev(src, bb_length)
            upper = basis + dev
            lower = basis - dev
            
            PB_upperCond = Conds.Standards.two_line_pos(
                line1=src,
                line2=upper,
                direction=bb_upper_method,
                equal=True,
                use_flag=bb_upper_use_flag
            )
            
            PB_lowerCond = Conds.Standards.two_line_pos(
                line1=src,
                line2=lower,
                direction=bb_lower_method,
                equal=True,
                use_flag=bb_lower_use_flag
            )
            
            cond = Utils.combine_conditions([range_cond, PB_upperCond, PB_lowerCond])
            
            return cond
        
        @staticmethod
        def trend(
            df: pd.DataFrame,
            src_name: str, 
            calc_type: str= "QoQ",
            rolling_len: int = 1, 
            direction: str = "increase",
            number_of_quarters: float = 3,
            growth: str = 'acceleration', # deceleration
            use_flag: bool = False, 
            *args, **kwargs
        ):  
            if use_flag:
                src = df[src_name]
                df['rollSum'] = src.rolling(rolling_len).sum()
                
                if calc_type == 'QoQ':
                    df['percentageChange'] = (df['rollSum'] - df['rollSum'].shift(1))/ df['rollSum'].shift(1).abs()  * 100
                else:
                    df['percentageChange'] = (df['rollSum'] - df['rollSum'].shift(4))/ df['rollSum'].shift(4).abs()  * 100

                df['secondOrderChange'] = df['percentageChange'].diff()
                df['positiveCount'] =  (df['secondOrderChange']>0).rolling(number_of_quarters-1).sum()
                df['negativeCount'] =  (df['secondOrderChange']<0).rolling(number_of_quarters-1).sum()

                if growth == 'acceleration':
                    df['growth'] = df['positiveCount'] == number_of_quarters -1
                elif growth == 'deceleration':
                    df['growth'] = df['negativeCount'] == number_of_quarters -1
                else:
                    df['growth'] = True
                
                if direction == 'increase':
                    df['direction'] = df['percentageChange'].rolling(number_of_quarters).max() > 0
                elif direction == 'decrease':
                    df['direction'] = df['percentageChange'].rolling(number_of_quarters).min() < 0
                else:
                    df['direction'] = True
                df['cond'] = df['direction'] & df['growth']
                return df['cond']
            
            return None
        
        @staticmethod
        def growth(
            df: pd.DataFrame,
            src_name: str,
            use_flag: bool = False,
            calc_type: str = "QoQ",
            rolling_len: int = 1,
            direction: str = 'increase', 
            percentage: float = 0, 
            *args, **kwargs
        ):
            if use_flag:
            
                df['rollSum'] = df[f'{src_name}'].rolling(rolling_len).sum()
                
                if calc_type == 'QoQ':
                    df['percentageChange'] = (df['rollSum'] - df['rollSum'].shift(1))/ df['rollSum'].shift(1).abs()  * 100
                else:
                    df['percentageChange'] = (df['rollSum'] - df['rollSum'].shift(4))/ df['rollSum'].shift(4).abs()  * 100
                    
                if direction == 'increase':
                    df['cond'] = df['percentageChange'] > percentage
                else:
                    df['cond'] = df['percentageChange'] < -percentage
                    
                return df['cond']
            
            return None
            
        @staticmethod
        def inventory(
            df: pd.DataFrame,
            hlest_use_flag: bool = False,
            hlest_direction: str = 'highest', 
            hlest_n_quarters: int = 3,
            
            std_use_flag: bool = False,
            std_dir: str = 'higher', 
            std_mult: float = 2,
            std_periods: int = 10,
            
            use_shift = False,
            n_shift = 15, 
            *args, **kwargs
        ):
            def test():
                df = glob_obj.get_one_stock_data("HPG")
                
                hlest_use_flag: bool = False
                hlest_direction: str = 'highest' 
                hlest_n_quarters: int = 3
                
                std_use_flag: bool = True
                std_dir: str = 'higher' 
                std_mult: float = 2
                std_periods: int = 10
                
            df_inv = df.groupby('mapYQ').agg({'inventory':'last'})
            
            hlest_cond = Conds.Standards.hlest_cond(
                df_inv,
                src_name='inventory', 
                hl_options=hlest_direction,
                n_bars=hlest_n_quarters,
                use_flag=hlest_use_flag
            )
            
            std_cond = Conds.Standards.std_cond(
                df = df_inv,
                src_name='inventory',
                n_bars= std_periods,
                mult=std_mult,
                position=std_dir,
                use_flag=std_use_flag
            )
            
            matched = Utils.combine_conditions([hlest_cond, std_cond])
            
            if matched is not None:
                df_inv['cond'] = matched
                df['cond'] = df['mapYQ'].map(df_inv['cond'])
                
                cond = df['cond']
                if use_shift:
                    cond = cond.shift(n_shift)
                    cond = cond.fillna(value=False)
                
                return cond
            
            return None
        
        @staticmethod
        def inventory_day(
            df: pd.DataFrame,
            hlest_use_flag: bool = False,
            hlest_direction: str = 'highest', 
            hlest_n_quarters: int = 3,
            
            std_use_flag: bool = False,
            std_dir: str = 'higher', 
            std_mult: float = 2,
            std_periods: int = 10,
            
            use_shift = False,
            n_shift = 15, 
            *args, **kwargs
        ):
            def test():
                df = glob_obj.get_one_stock_data("HPG")
                
                hlest_use_flag: bool = False
                hlest_direction: str = 'highest' 
                hlest_n_quarters: int = 3
                
                std_use_flag: bool = True
                std_dir: str = 'higher' 
                std_mult: float = 2
                std_periods: int = 10
                
            df_inv = df.groupby('mapYQ').agg({'inventoryDay':'last'})
            
            hlest_cond = Conds.Standards.hlest_cond(
                df_inv,
                src_name='inventoryDay', 
                hl_options=hlest_direction,
                n_bars=hlest_n_quarters,
                use_flag=hlest_use_flag
            )
            
            std_cond = Conds.Standards.std_cond(
                df = df_inv,
                src_name='inventoryDay',
                n_bars= std_periods,
                mult=std_mult,
                position=std_dir,
                use_flag=std_use_flag
            )
            
            matched = Utils.combine_conditions([hlest_cond, std_cond])
            
            if matched is not None:
                df_inv['cond'] = matched
                df['cond'] = df['mapYQ'].map(df_inv['cond'])
                
                cond = df['cond']
                if use_shift:
                    cond = cond.shift(n_shift)
                    cond = cond.fillna(value=False)
                
                return cond
            
            return None
        
        @staticmethod
        def capex_gross(
            df: pd.DataFrame,
            use_flag: bool = False,
            low_range: float = -999,
            high_range: float = 999,
            
            use_shift = False,
            n_shift = 15, 
            *args, **kwargs
        ):
            def test():
                df = df_raw.copy()
            src = -df['CAPEX'] / df['gross']
            cond =  Conds.Standards.range_cond(
                src,
                lower_thres=low_range,
                upper_thres=high_range,
                use_flag=use_flag
            )
            
            if use_shift and cond is not None:
                cond = cond.shift(n_shift)
                cond = cond.fillna(value=False)
            
            return cond
            
        @staticmethod
        def cash_debt_mktcap(
            df: pd.DataFrame,
            use_flag: bool = False,
            low_range: float = -999,
            high_range: float = 999,
            
            use_shift = False,
            n_shift = 15, 
            *args, **kwargs
        ):
            src = (df['cash'] - df['debt']) / (df['marketCap'] * 1e9)
            cond = Conds.Standards.range_cond(
                src,
                lower_thres=low_range,
                upper_thres=high_range,
                use_flag=use_flag
            )
            
            if use_shift and cond is not None:
                cond = cond.shift(n_shift)
                cond = cond.fillna(value=False)
            
            return cond
            
            
            
    class Sectors:
        @staticmethod
        def steel_ma_and_hl(
            df: pd.DataFrame, 
            steel_src,
            show_src: bool = False,
            ma_use_flag: bool = False, # Coking Coal MA combination
            ma_type = 'EMA', # Method
            ma_len1 = 5, # MA1
            ma_len2 = 15, # MA2
            range_use_flag: bool = False,
            range_ma_type = 'EMA',
            range_ma_len = 5,
            range_lower = -999,
            range_upper = 999,
            ma_dir = 'crossover', # Direction
            hl_use_flag: bool = False, # Increase/Decrease over N bars
            hl_nbars = 5, # N Bars
            hl_dir = 'Increase', # Direction
            hl_lower = -999, # Lower
            hl_upper = 999, # Upper
            *args, **kwargs
        ):
            
            def test():
                steel_src='Margin'
                ma_use_flag: bool = False # Coking Coal MA combination
                ma_type = 'EMA' # Method
                ma_len1 = 5 # MA1
                ma_len2 = 15 # MA2
                ma_dir = 'crossover' # Direction
                hl_use_flag: bool = True # Increase/Decrease over N bars
                hl_nbars = 10 # N Bars
                hl_dir = 'Increase' # Direction
                hl_lower = 15 # Lower
                hl_upper = 999 # Upper
                df = glob_obj.get_one_stock_data("HPG")
        
            df_steel = glob_obj.sectors['steel']['io_data'].copy()
            df_steel['high'] = df_steel['low'] = df_steel[steel_src].copy()
            df_steel = df_steel[df_steel['day'].isin(df['day'])]
            
            if show_src:
                df[steel_src] = df['day'].map(dict(zip(df_steel['day'], df_steel[steel_src])))
            
            ma_cond = Conds.Standards.two_ma_lines(
                df_steel,
                src_name=steel_src,
                ma_len1=ma_len1,
                ma_len2=ma_len2,
                ma_type=ma_type,
                ma_dir = ma_dir,
                use_flag=ma_use_flag
            )
            
            range_line = Ta.ma(df_steel[steel_src], range_ma_len, range_ma_type)
            range_cond = Conds.Standards.range_cond(
                line = range_line,
                lower_thres= range_lower,
                upper_thres= range_upper,
                use_flag= range_use_flag
            )

            hl_cond = Conds.price_change_vs_hl(
                df_steel,
                src_name=steel_src,
                use_flag=hl_use_flag,
                direction=hl_dir,
                nbars=hl_nbars,
                low_range=hl_lower,
                high_range=hl_upper
            )

            steel_cond = Utils.combine_conditions([ma_cond, hl_cond, range_cond])
            if steel_cond is not None:
                df_steel['cond'] = steel_cond
                return Utils.merge_condition(df, dict(zip(df_steel['day'], df_steel['cond'])))
            
            return None
        
        @staticmethod
        def coking_coal(*args, **kwargs):
            return Conds.Sectors.steel_ma_and_hl(*args, steel_src='Aus Coal', **kwargs)
        
        @staticmethod
        def iron_ore(*args, **kwargs):
            return Conds.Sectors.steel_ma_and_hl(*args, steel_src='Ore 62', **kwargs)
        
        @staticmethod
        def china_hrc(*args, **kwargs):
            return Conds.Sectors.steel_ma_and_hl(*args, steel_src='China HRC', **kwargs)
        
        @staticmethod
        def china_rebar(*args, **kwargs):
            return Conds.Sectors.steel_ma_and_hl(*args, steel_src='China Long steel', **kwargs)
        
        @staticmethod
        def steel_scrap(*args, **kwargs):
            return Conds.Sectors.steel_ma_and_hl(*args, steel_src='Scrap', **kwargs)
        
        @staticmethod
        def hpg_margin(*args, **kwargs):
            return Conds.Sectors.steel_ma_and_hl(*args, steel_src='Margin', **kwargs)

        @staticmethod
        def brokerage_margin(
            df: pd.DataFrame,
            ma_use_flag: bool = False, 
            ma_type: str = 'SMA', 
            ma_len1: int = 5,
            ma_len2: int = 15,
            ma_dir: str = 'crossover',
            range_use_flag: bool = False,
            range_lower: float = -999,
            range_upper: float = 999,
            hl_use_flag: bool = False,
            hl_options: str = 'highest',
            hl_nbars: int = 4,
            
            use_shift = False,
            n_shift = 15, 
            *args, **kwargs
        ):
            def test():
                df = glob_obj.get_one_stock_data("VND")
                ma_use_flag: bool = True 
                ma_type: str = 'SMA' 
                ma_len1: int = 5
                ma_len2: int = 15
                ma_dir: str = 'crossover'
                range_use_flag: bool = False
                range_lower: float = -999
                range_upper: float = 999
                hl_use_flag: bool = False
                hl_options: str = 'highest'
                hl_nbars: int = 4
            
            df_mg = df.groupby("mapYQ")[["marginLending"]].max()
            
            ma1 = Ta.ma(src=df_mg, length=ma_len1, ma_type=ma_type)
            ma2 = Ta.ma(src=df_mg, length=ma_len2, ma_type=ma_type)
            ma_cond = Conds.Standards.two_line_conditions(
                line1=ma1, 
                line2=ma2, 
                use_vs_signal=ma_use_flag,
                direction=ma_dir,
                use_range=range_use_flag,
                lower_thres=range_lower,
                upper_thres=range_upper,
            )
            
            def hl_cond():
                if hl_use_flag:
                    func = Ta.is_highest if hl_options == 'highest' else Ta.is_lowest
                    df_mg['matched'] = func(df_mg['marginLending'], hl_nbars)
                    return df_mg['matched']

                return None
            
            matched = Utils.combine_conditions([ma_cond, hl_cond()])
            if matched is not None:
                df_mg['cond'] = matched
                df['cond'] = df['mapYQ'].map(df_mg['cond'])
                
                cond = df['cond']
                if use_shift:
                    cond = cond.shift(n_shift)
                    cond = cond.fillna(value=False)
            
                return cond
            
            return None
        
        @staticmethod
        def brokerage_deposit_rate(
            df: pd.DataFrame,
            ma_use_flag: bool = False,
            ma_type: str = 'EMA', 
            ma_len1: int = 5,
            ma_len2: int = 15,
            ma_dir: str = 'crossover', 
            range_use_flag: bool = False,
            range_lower: float = -9999,
            range_upper: float = 9999,

            pct_ema_len1: int = 5,
            pct_ema_len2: int = 15,
            pct_period: int = 128,
            pct_thres_use_flag: bool = False,
            pct_thres_pct: float = 90,
            pct_thres_dir: str = 'higher', # 'higher', 'lower' only
            pct_madir_use_flag: bool = False,
            pct_madir: str = 'EMA1 > EMA2', # 'EMA1 > EMA2' or 'EMA1 < EMA2'
            *args, **kwargs
        ):
            def test():
                ma_len1: int = 5
                ma_len2: int = 15
                ma_type: str = 'EMA'
                df = df_raw.copy()
                ma_use_flag: bool = True
                ma_type: str = 'EMA' 
                ma_len1: int = 5
                ma_len2: int = 15
                ma_dir: str = 'crossover' 
                range_use_flag: bool = False
                range_lower: float = -9999
                range_upper: float = 9999

                pct_ema_len1: int = 5
                pct_ema_len2: int = 15
                pct_period: int = 128
                pct_thres_use_flag: bool = False
                pct_thres_pct: float = 90
                pct_thres_dir: str = 'higher' # 'higher' 'lower' only
                pct_madir_use_flag: bool = False
                pct_madir: str = 'EMA1 > EMA2' # 'EMA1 > EMA2' or 'EMA1 < EMA2'
                
            df_brokerage = glob_obj.sectors['brokerage']['io_data']
            df = df.copy()

            src = df_brokerage['depositRate']
            df['drma1'] = df['day'].map(dict(zip(df_brokerage['day'], Ta.ma(src=src, length=ma_len1, ma_type=ma_type)))).ffill()
            df['drma2'] = df['day'].map(dict(zip(df_brokerage['day'], Ta.ma(src=src, length=ma_len2, ma_type=ma_type)))).ffill()
            df['drema1'] = df['day'].map(dict(zip(df_brokerage['day'], Ta.ma(src=src, length=pct_ema_len1, ma_type='EMA')))).ffill()
            df['drema2'] = df['day'].map(dict(zip(df_brokerage['day'], Ta.ma(src=src, length=pct_ema_len2, ma_type='EMA')))).ffill()
            
            
            ma_cond = Conds.Standards.two_line_conditions(
                line1=df['drma1'], 
                line2=df['drma2'], 
                use_vs_signal=ma_use_flag,
                direction=ma_dir,
                use_range=range_use_flag,
                lower_thres=range_lower,
                upper_thres=range_upper,
            )
            
            df['gap'] =  df['drema1'] - df['drema2']
            df['per'] = Ta.rolling_rank(abs(df['gap']), pct_period)
            def thres_cond():
                if pct_thres_use_flag:
                    cond =  df['per'] >= pct_thres_pct if pct_thres_dir == 'higher' else df['per'] < pct_thres_pct
                    return cond
                
                return None
            
            def dir_cond():
                if pct_madir_use_flag:
                    cond = df['gap'] > 0 if pct_madir == 'EMA1 > EMA2' else  df['gap'] < 0
                    return cond
                
                return None
            
            cond = Utils.combine_conditions([ma_cond, thres_cond(), dir_cond()])
            
            return cond
        

        @staticmethod
        def check_5sectors_macrodata(
            df: pd.DataFrame,
            sector:str,
            field: str,
            ma_use_flag: bool = False,
            ma_type: str = 'SMA',
            ma_len1: int = 5,
            ma_len2: int = 15,
            ma_dir: str = 'crossover', 
            range_use_flag: bool = False,
            range_ma_type: str = 'EMA',
            range_ma_len: int = 5,
            range_lower: float = -9999,
            range_upper: float = 9999,
            change_use_flag: bool = False,
            change_periods: int = 10, 
            change_dir: str = 'increase', #dercrease
            change_lrange: float = 0,
            change_hrange: float = 100,
            hlest_use_flag: bool = False,
            hlest_dir: str = 'highest', # lowest
            hlest_periods: int = 10,
            std_use_flag: bool = False,
            std_dir: str = 'higher', 
            std_mult: float = 2,
            std_periods: int = 10, 
            *args, **kwargs
        ):
            def test():
                df = df_raw.copy()
                sector = 'fish'
                field = 'rawFishPrice'
                ma_use_flag: bool = True
                ma_len1: int = 5
                ma_len2: int = 15
                ma_dir: str = 'crossover' 
                change_use_flag: bool = False
                change_periods: int = 10 
                change_dir: str = 'increase' #dercrease
                change_lrange: float = 0
                change_hrange: float = 100
                hlest_use_flag: bool = False
                hlest_dir: str = 'highest' # lowest
                hlest_periods: int = 10
                std_use_flag: bool = False
                std_dir: str = 'higher' 
                std_mult: float = 2
                std_periods: int = 10
                
            df_sector: pd.DataFrame = glob_obj.sectors[sector]['io_data'].copy()
            df_sector = df_sector[~df_sector[field].isna()]
            
            ma_cond = Conds.Standards.two_ma_lines(
                df=df_sector,
                src_name=field,
                ma_len1=ma_len1,
                ma_len2=ma_len2,
                ma_type=ma_type,
                ma_dir=ma_dir,
                use_flag=ma_use_flag
            )
            
            range_line = Ta.ma(df_sector[field], range_ma_len, range_ma_type)
            range_cond = Conds.Standards.range_cond(
                line = range_line,
                lower_thres= range_lower,
                upper_thres= range_upper,
                use_flag= range_use_flag
            )
            
            change_cond = Conds.price_change(
                df=df_sector,
                src_name=field,
                periods=change_periods,
                direction=change_dir,
                lower_thres=change_lrange,
                upper_thres=change_hrange,
                use_flag=change_use_flag
            )
            
            hlest_cond = Conds.Standards.hlest_cond(
                df=df_sector,
                src_name=field,
                hl_options=hlest_dir,
                n_bars=hlest_periods,
                use_flag=hlest_use_flag
            )
            
            std_cond = Conds.Standards.std_cond(
                df = df_sector,
                src_name=field,
                n_bars= std_periods,
                mult=std_mult,
                position=std_dir,
                use_flag=std_use_flag
            )
            
            matched = Utils.combine_conditions([ma_cond, change_cond, hlest_cond, std_cond, range_cond])

            if matched is not None:
                df_sector['cond'] = matched
                cond = Utils.merge_condition(df, dict(zip(df_sector['day'], df_sector['cond'])), fillna=False)
                cond = cond.ffill().fillna(False)
                return cond
            
            return  None

        @staticmethod
        def vhc_us_asp(*args, **kwargs):
            return Conds.Sectors.check_5sectors_macrodata( *args, sector='fish', field='vhcUsAsp', **kwargs)
        
        @staticmethod
        def raw_fish_price(*args, **kwargs):
            return Conds.Sectors.check_5sectors_macrodata( *args, sector='fish', field='rawFishPrice', **kwargs)
        
        @staticmethod
        def us_price_spread(*args, **kwargs):
            return Conds.Sectors.check_5sectors_macrodata( *args, sector='fish', field='usPriceSpread', **kwargs)
        
        @staticmethod
        def north_hog_price(*args, **kwargs):
            return Conds.Sectors.check_5sectors_macrodata( *args, sector='hog', field='avgNorthHogPrice', **kwargs)
        
        @staticmethod
        def south_hog_price(*args, **kwargs):
            return Conds.Sectors.check_5sectors_macrodata( *args, sector='hog', field='avgSouthHogPrice', **kwargs)
        
        @staticmethod
        def hog_feed_cost(*args, **kwargs):
            return Conds.Sectors.check_5sectors_macrodata( *args, sector='hog', field='hogFeedCost', **kwargs)
        
        @staticmethod
        def hog_cash_spread(*args, **kwargs):
            return Conds.Sectors.check_5sectors_macrodata( *args, sector='hog', field='hogCashSpread', **kwargs)
        
        @staticmethod
        def china_p4_price(*args, **kwargs):
            return Conds.Sectors.check_5sectors_macrodata( *args, sector='fertilizer', field='ChinaP4Price', **kwargs)
        
        @staticmethod
        def china_p4_cash_spread(*args, **kwargs):
            return Conds.Sectors.check_5sectors_macrodata( *args, sector='fertilizer', field='ChinaP4CashSpread', **kwargs)
        
        @staticmethod
        def dcm_ure(*args, **kwargs):
            return Conds.Sectors.check_5sectors_macrodata( *args, sector='fertilizer', field='DCM', **kwargs)
        
        @staticmethod
        def dpm_ure(*args, **kwargs):
            return Conds.Sectors.check_5sectors_macrodata( *args, sector='fertilizer', field='DPM', **kwargs)
        
        @staticmethod
        def ure_future(*args, **kwargs):
            return Conds.Sectors.check_5sectors_macrodata( *args, sector='fertilizer', field='future', **kwargs)


    @staticmethod
    def compute_any_conds(df: pd.DataFrame, functions_params_dic: dict, *args, **kwargs):
        """Compute and combine any conditions"""
        # df = df.copy()
        conds = []
        use_shift = kwargs.get("use_shift", False)
        n_shift = kwargs.get("n_shift", 15)
        try:
            for func_name, params in functions_params_dic.items():
                try:
                    if func_name == 'stock_scanner':
                        continue
                    if(func_name not in glob_obj.function_map): 
                        continue
                    if func_name in glob_obj.fa_funcs:
                        params['use_shift'] = use_shift
                        params['n_shift'] = n_shift
                    func = glob_obj.function_map[func_name]
                    cond = func(df, **params)
                except Exception as fe:
                    cond = None
                    logging.error(f"function `{func_name}` error: {fe}")
                    
                if cond is not None:
                    conds.append(cond)
        except Exception as e:
            conds = []
            logging.error(f"function `compute_any_conds` error: {e}")

        if len(conds) > 0:
            return Utils.combine_conditions(conds)

        return None

    @staticmethod
    def index_cond(df: pd.DataFrame, *args, **index_params):
        """Compute index condition"""
        
        def test():
            index_params = {
                'price_change': {
                    'lower_thres': 3,
                    'use_flag': True
                }
            }
            df = glob_obj.get_one_stock_data('HPG')
        
        df2 = glob_obj.get_one_stock_data('VNINDEX')

        index_cond = Conds.compute_any_conds(df2, index_params)
        
        if index_cond is not None:
            df2["index_cond"] = index_cond
            close = df['close']
            df = pd.merge(df, df2, how="left", on="day")
            df["index_cond"] = np.where(
                df["index_cond"].isna(), False, df["index_cond"]
            )
            
            df["index_cond"] = np.where(close.isna(),False, df['index_cond'])
            df = df.infer_objects()
            return df["index_cond"]

        return None

    @staticmethod
    def lookback_cond(df: pd.DataFrame, n_bars: int, *args, **lookback_params: dict):
        """Compute lookback condition"""
        def test():
            df: pd.DataFrame = df_raw.copy()
            n_bars = 5
            lookback_params =  {
                'lookback_cond':{
                    'price_change':{
                        'periods':  2,
                        'direction':  "increase",
                        'lower_thres':  6,
                        'upper_thres':  100,
                        'use_flag':  True
                    },
                    'bbpctb':{
                        'length': 20,
                        'mult':  2.5,
                        'use_range':  False,
                        'low_range':  80,
                        'high_range':  100,
                        'use_cross':  True,
                        'direction':  "crossover",
                        'cross_line':  "Lower band",
                        'use_flag':  True
                    }
                }
            }
            
        use_shift = lookback_params.get('use_shift', False)
        n_shift = lookback_params.get("n_shift", 15)


        conds = []
        for func_name, func_params in lookback_params.items():
            try:
                if func_name == 'stock_scanner':
                    continue
                if(func_name not in glob_obj.function_map): 
                    continue
                if func_name in glob_obj.fa_funcs:
                    func_params['use_shift'] = use_shift
                    func_params['n_shift'] = n_shift
                    
                func = glob_obj.function_map[func_name]
                cond: pd.Series = func(df, **func_params)
            except Exception as fe:
                cond = None
                logging.error(f"lookback function `{func_name}` error: {fe}")
                    
            if cond is not None:
                cond = cond.rolling(n_bars, closed = 'left').max().fillna(False).astype(bool)
                conds.append(cond)

        if len(conds) > 0:
            return Utils.combine_conditions(conds)

        return None
    
    @staticmethod
    def consecutive_lower(df: pd.DataFrame, src_name, compare_with, n_bar):
        src = df[src_name]
        src2 = df[compare_with]
        res = None
        "...."
        return res


    @staticmethod 
    def min_inc_dec_bars_old(
        src: pd.Series,
        n_bars: int = 10,
        n_bars_inc: int = None,
        n_bars_dec: int = None,
        use_flag: bool = True,
        *args, **kwargs
    ) -> pd.Series:
        """Check if within the latest n_bars, there are at least n_bars_inc increasing bars
        and n_bars_dec decreasing bars. Uses vectorized operations.

        Args:
            src (pd.Series): Input source series
            n_bars (int, optional): Number of bars to look back. Defaults to 10.
            n_bars_inc (int, optional): Minimum number of increasing bars required. 
                Defaults to None.
            n_bars_dec (int, optional): Minimum number of decreasing bars required.
                Defaults to None. 
            use_flag (bool, optional): Whether to perform calculation. Defaults to True.

        Returns:
            pd.Series: Boolean series where True means conditions are met
        """
        if not use_flag:
            return None

        # Input validation
        if n_bars_inc is None and n_bars_dec is None:
            raise ValueError("At least one of n_bars_inc or n_bars_dec must be specified")

        if n_bars_inc is not None and n_bars_inc > n_bars:
            raise ValueError("n_bars_inc cannot be greater than n_bars")

        if n_bars_dec is not None and n_bars_dec > n_bars:
            raise ValueError("n_bars_dec cannot be greater than n_bars")

        # Calculate changes
        changes = src.diff()
        
        # Calculate rolling counts of increasing and decreasing bars
        inc_count = changes.rolling(n_bars).apply(lambda x: (x > 0).sum())
        dec_count = changes.rolling(n_bars).apply(lambda x: (x < 0).sum())
        
        # Create conditions based on requirements
        inc_condition = True if n_bars_inc is None else inc_count >= n_bars_inc
        dec_condition = True if n_bars_dec is None else dec_count >= n_bars_dec
        
        # Combine conditions
        result = inc_condition & dec_condition
        
        # Handle NaN values from rolling window startup
        result = result.fillna(False)
        
        return result
    
    @staticmethod
    def min_inc_dec_bars(
        src: pd.Series,
        n_bars: int = 10,
        n_bars_inc: int = None,
        n_bars_dec: int = None,
        use_flag: bool = True,
        *args, **kwargs
    ) -> pd.Series:
        if not use_flag:
            return None
        
        if n_bars_inc is None and n_bars_dec is None:
            raise ValueError("At least one of n_bars_inc or n_bars_dec must be specified")
        
        if n_bars_inc is not None and n_bars_inc > n_bars:
            raise ValueError("n_bars_inc cannot be greater than n_bars")
        
        if n_bars_dec is not None and n_bars_dec > n_bars:
            raise ValueError("n_bars_dec cannot be greater than n_bars")
        
        changes: pd.Series = src.diff()
        
        inc_mask = (changes > 0).astype(int)
        dec_mask = (changes < 0).astype(int)
        
        inc_cumsum = inc_mask.cumsum()
        dec_cumsum = dec_mask.cumsum()
        
        inc_count = inc_cumsum - inc_cumsum.shift(n_bars).fillna(0)
        dec_count = dec_cumsum - dec_cumsum.shift(n_bars).fillna(0)
        
        inc_condition = True if n_bars_inc is None else inc_count >= n_bars_inc
        dec_condition = True if n_bars_dec is None else dec_count >= n_bars_dec
        
        result = inc_condition & dec_condition
        
        result.iloc[:n_bars-1] = False
        
        return result

    @staticmethod
    def percent_change_in_range(
        src: pd.Series,
        n_bars: int = 10,
        lower_thres: float = 0,
        upper_thres: float = 100,
        use_flag: bool = True,
        use_absolute: bool = False,
        *args, **kwargs
    ) -> pd.Series:
        """Check if percentage change over n_bars falls within specified range.
        
        Args:
            src (pd.Series): Input source series
            n_bars (int): Lookback period for calculating change
            lower_thres (float): Lower threshold for change
            upper_thres (float): Upper threshold for change
            use_flag (bool): Whether to perform calculation
            use_absolute (bool): Whether to use absolute change
            
        Returns:
            pd.Series: Boolean series where True means conditions are met
        """
        if not use_flag:
            return None
            
        if lower_thres > upper_thres:
            raise ValueError("lower_thres must be less than or equal to upper_thres")

        # Calculate percentage change
        if use_absolute:
            pct_change = src.diff(periods=n_bars)
        else:
            pct_change = src.pct_change(periods=n_bars) * 100

        # Check if within range
        result = (pct_change >= lower_thres) & (pct_change <= upper_thres)
        
        # Handle NaN values from the rolling calculation
        result = result.fillna(False)
        
        return result


class Simulator:
    """Backtest class"""

    def __init__(
        self,
        func=None,
        df_ohlcv: pd.DataFrame = None,
        params: dict=None,
        exit_params: dict=None,
        name: str = "",
        description: str = ""
    ):
        self.func = func
        self.df = df_ohlcv
        self.params = params
        self.exit_params = exit_params
        self.name = name
        self.description = description
        self.result = pd.DataFrame()

    @staticmethod
    def prepare_data_for_backtesting(
        df: pd.DataFrame, trade_direction="Long", holding_periods=8
    ):
        """
        Chuẩn bị dữ liệu (entryDay, entryPrice, exitDay, exitPrice) và tính toán lợi nhuận cho các kỳ hạn khác nhau.                                                     
        ► Được dùng bởi: 'run', 'run3' (không xét params: profit_thres và loss_thres)
        """
        df = df.copy()
        try:
            df["entryDay"] = df["day"].copy()
            df["entryDay"] = df["entryDay"].shift(-1)
            df["entryType"] = 1 if trade_direction == "Long" else -1
            df["entryPrice"] = df["open"].shift(-1).copy()
            df["exitDay"] = df["entryDay"].shift(-holding_periods)
            df["exitType"] = df["entryType"] * -1
            df["exitPrice"] = df["entryPrice"].shift(-holding_periods)

            df["priceChange"] = np.where(
                trade_direction == "Long",
                df["exitPrice"] - df["entryPrice"],
                df["entryPrice"] - df["exitPrice"],
            )
            df["return"] = df["priceChange"] / df["entryPrice"] * 100
            df["return1"]  =( (df["entryPrice"].shift(-1)  / df["entryPrice"]) - 1) * 100
            df["return5"]  =( (df["entryPrice"].shift(-5)  / df["entryPrice"]) - 1) * 100
            df["return10"] =( (df["entryPrice"].shift(-10) / df["entryPrice"]) - 1) * 100
            df["return15"] =( (df["entryPrice"].shift(-15) / df["entryPrice"]) - 1) * 100
            
            df["downside"] = (
                df["low"].rolling(holding_periods).min().shift(-holding_periods)
            )
            df["downside"] = (
                df[["downside", "exitPrice"]].min(axis=1) / df["entryPrice"] - 1
            ) * 100

            df["upside"] = df["high"].rolling(holding_periods).max().shift(-holding_periods)
            df["upside"] = (
                df[["upside", "exitPrice"]].max(axis=1) / df["entryPrice"] - 1
            ) * 100
        except Exception as e:
            logging.error(f"Function `prepare_data_for_backtesting` had met error: {e}")
            df = None
        return df
    
    @staticmethod
    @njit
    def compute_trade_stats(
        price_changes, 
        returns, 
        returns1,
        returns5,
        returns10,
        returns15,
        signals, 
        upsides, 
        downsides, 
        holding_periods
    ):
        """Computing trade stats, using numba"""
        num_trades = 0
        num_win = 0
        num_win1 = 0
        num_win5 = 0
        num_win10 = 0
        num_win15 = 0

        
        
        num_loss = 0
        total_profit = 0.0
        total_loss = 0.0
        total_return = 0.0
        total_return1 = 0.0
        total_return5 = 0.0
        total_return10 = 0.0
        total_return15 = 0.0
        
        
        total_upside = 0.0
        total_downside = 0.0
        
        max_runup = np.NaN
        max_drawdown = np.NaN

        is_trading = False
        entry_idx = 0
        trading_status = np.full(len(signals), np.NaN)
        num_trades_arr = np.full(len(signals), np.NaN)
        
        winrate_arr = np.full(len(signals), np.NaN)
        winrate1_arr = np.full(len(signals), np.NaN)
        winrate5_arr = np.full(len(signals), np.NaN)
        winrate10_arr = np.full(len(signals), np.NaN)
        winrate15_arr = np.full(len(signals), np.NaN)
        
        avg_returns_arr = np.full(len(signals), np.NaN)
        avg_returns1_arr = np.full(len(signals), np.NaN)
        avg_returns5_arr = np.full(len(signals), np.NaN)
        avg_returns10_arr = np.full(len(signals), np.NaN)
        avg_returns15_arr = np.full(len(signals), np.NaN)
        
        profit_factor_arr = np.full(len(signals), np.NaN)
        is_entry_arr = np.full(len(signals), np.NaN)

        avg_upside_arr = np.full(len(signals), np.NaN)
        avg_downside_arr = np.full(len(signals), np.NaN)
        
        max_runup_arr = np.full(len(signals), np.NaN)
        max_drawdown_arr = np.full(len(signals), np.NaN)

        for index, signal in enumerate(signals):
            if signal and not is_trading:
                is_trading = True
                entry_idx = index

                trade_return = returns[index]

                if not np.isnan(trade_return):
                    num_trades += 1
                    trade_profit = price_changes[index]

                    total_return += trade_return
                    total_return1 += returns1[index]
                    total_return5 += returns5[index]
                    total_return10 += returns10[index]
                    total_return15 += returns15[index]
                    
                    total_upside += upsides[index]
                    total_downside += downsides[index]
                    
                    # max runup, max drawdown
                    if np.isnan(max_runup):
                        max_runup = upsides[index]
                    else:
                        max_runup = max(max_runup, upsides[index])
                        
                    if np.isnan(max_drawdown):
                        max_drawdown = downsides[index]
                    else:
                        max_drawdown = min(max_drawdown, downsides[index])
                    
                    if trade_profit > 0:
                        num_win += 1
                        total_profit += trade_profit
                    else:
                        num_loss += 1
                        total_loss += trade_profit
                        
                    num_win1  += 1 if returns1[index]  > 0 else 0
                    num_win5  += 1 if returns5[index]  > 0 else 0
                    num_win10 += 1 if returns10[index] > 0 else 0
                    num_win15 += 1 if returns15[index] > 0 else 0

                    num_trades_arr[index] = num_trades
                    
                    winrate_arr[index] = num_win / num_trades * 100
                    winrate1_arr[index] = num_win1 / num_trades * 100
                    winrate5_arr[index] = num_win5 / num_trades * 100
                    winrate10_arr[index] = num_win10 / num_trades * 100
                    winrate15_arr[index] = num_win15 / num_trades * 100
                    
                    avg_returns_arr[index] = total_return / num_trades
                    avg_returns1_arr[index] = total_return1 / num_trades
                    avg_returns5_arr[index] = total_return5 / num_trades
                    avg_returns10_arr[index] = total_return10 / num_trades
                    avg_returns15_arr[index] = total_return15 / num_trades
                    
                    avg_upside_arr[index] = total_upside / num_trades
                    avg_downside_arr[index] = total_downside / num_trades
                    
                    max_runup_arr[index] = max_runup
                    max_drawdown_arr[index] = max_drawdown
                    
                    profit_factor_arr[index] = (
                        total_profit / (total_loss * -1) if total_loss != 0 else np.NaN
                    )

                is_entry_arr[index] = 1

            if is_trading and (index - entry_idx == holding_periods):
                is_trading = False

            if is_trading:
                trading_status[index] = 1

        return (
            trading_status,
            num_trades_arr,
            winrate_arr,
            winrate1_arr,
            winrate5_arr,
            winrate10_arr,
            winrate15_arr,
            avg_returns_arr,
            avg_returns1_arr,
            avg_returns5_arr,
            avg_returns10_arr,
            avg_returns15_arr,
            avg_upside_arr,
            avg_downside_arr,
            max_runup_arr,
            max_drawdown_arr,
            profit_factor_arr,
            is_entry_arr,
        )

    @staticmethod
    def compute_trade_stats2(
        day,
        open_p, 
        high_p,
        low_p,
        close_p,
        open_i,
        high_i,
        low_i,
        close_i,
        entry_signals,
        exit_signals,
        direction=1,
        use_holding_periods=False,
        holding_periods=15,
        use_takeprofit_cutloss=False,
        profit_thres=5,
        loss_thres=5
    ):
# %%
        def test():
            entry_condition = {
                'price_comp_ma':{
                    "ma_len1":5,
                    "ma_len2":15,
                    "ma_type":"EMA",
                    "ma_dir":"crossover",
                    "use_flag": True,
                }
            }

            exit_condition = {
                'price_comp_ma':{
                    "ma_len1":5,
                    "ma_len2":15,
                    "ma_type":"EMA",
                    "ma_dir":"below",
                    "use_flag": False,
                }
            }

            df = df_raw.copy()
            
            day = df['day'].astype(int).values
            open_p = df['open'].values
            high_p = df['high'].values
            low_p = df['low'].values
            entry_signals = Conds.compute_any_conds(df, entry_condition)
            exit_signals = Conds.compute_any_conds(df, exit_condition)
            
            direction = 1
            
            use_holding_periods=False
            holding_periods = 15
            
            use_takeprofit_cutloss=False
            profit_thres=5
            loss_thres=5
        
        num_trades = 0
        num_win = 0
        num_win1 = 0
        num_win5 = 0
        num_win10 = 0
        num_win15 = 0
        num_win_i = 0
        
        total_return = 0.0
        total_return1 = 0.0
        total_return5 = 0.0
        total_return10 = 0.0
        total_return15 = 0.0
        total_return_i = 0.0
        
        total_upside = 0
        total_downside = 0
        total_upside_i = 0
        total_downside_i = 0
        
        total_profit = 0.0
        total_loss = 0.0
        total_profit_i = 0.0
        total_loss_i = 0.0
        
        max_runup = np.NaN
        max_drawdown = np.NaN
        max_runup_i = np.NaN
        max_drawdown_i = np.NaN

        is_entry_arr = np.full(len(entry_signals), np.NaN)
        trading_status = np.full(len(entry_signals), np.NaN)
        
        entry_day_arr = np.full(len(entry_signals), np.NaN)
        entry_price_arr = np.full(len(entry_signals), np.NaN)

        exit_day_arr = np.full(len(entry_signals), np.NaN)
        exit_price_arr = np.full(len(entry_signals), np.NaN)
        
        price_change_arr = np.full(len(entry_signals), np.NaN)
        num_trades_arr = np.full(len(entry_signals), np.NaN)

        winrate_arr = np.full(len(entry_signals), np.NaN)
        winrate_arr1 = np.full(len(entry_signals), np.NaN)
        winrate_arr5 = np.full(len(entry_signals), np.NaN)
        winrate_arr10 = np.full(len(entry_signals), np.NaN)
        winrate_arr15 = np.full(len(entry_signals), np.NaN)

        
        return_arr = np.full(len(entry_signals), np.NaN)
        return_arr1 = np.full(len(entry_signals), np.NaN)
        return_arr5 = np.full(len(entry_signals), np.NaN)
        return_arr10 = np.full(len(entry_signals), np.NaN)
        return_arr15 = np.full(len(entry_signals), np.NaN)
        
        avg_returns_arr = np.full(len(entry_signals), np.NaN)
        avg_returns_arr1 = np.full(len(entry_signals), np.NaN)
        avg_returns_arr5 = np.full(len(entry_signals), np.NaN)
        avg_returns_arr10 = np.full(len(entry_signals), np.NaN)
        avg_returns_arr15 = np.full(len(entry_signals), np.NaN)

        
        profit_factor_arr = np.full(len(entry_signals), np.NaN)

        upside_arr = np.full(len(entry_signals), np.NaN)
        downside_arr = np.full(len(entry_signals), np.NaN)
        max_runup_arr = np.full(len(entry_signals), np.NaN)
        max_drawdown_arr = np.full(len(entry_signals), np.NaN)
        avg_upside_arr = np.full(len(entry_signals), np.NaN)
        avg_downside_arr = np.full(len(entry_signals), np.NaN)
        
        winrate_arr_i = np.full(len(entry_signals), np.NaN)
        avg_returns_arr_i = np.full(len(entry_signals), np.NaN)
        profit_factor_arr_i = np.full(len(entry_signals), np.NaN)
        
        return_arr_i = np.full(len(entry_signals), np.NaN)
        upside_arr_i = np.full(len(entry_signals), np.NaN)
        downside_arr_i = np.full(len(entry_signals), np.NaN)
        max_runup_arr_i = np.full(len(entry_signals), np.NaN)
        max_drawdown_arr_i = np.full(len(entry_signals), np.NaN)
        avg_upside_arr_i = np.full(len(entry_signals), np.NaN)
        avg_downside_arr_i = np.full(len(entry_signals), np.NaN)
        
        def src_idx(src, index):
            if index < len(src):
                return src[index]
            else:
                return np.NaN
        
        start_bar = 0
        is_trading = False

        trade_upside = np.NaN
        trade_downside = np.NaN
        entry_price = np.NaN
        
        trade_upside_i = np.NaN
        trade_downside_i = np.NaN
        entry_price_i = np.NaN
        
        for bar_index, signal in enumerate(entry_signals):
            if signal and not is_trading:
                is_trading = True
                start_bar = bar_index
                is_entry_arr[bar_index] = 1
                
                num_trades = num_trades + 1
                num_trades_arr[start_bar] = num_trades

                trade_upside = np.NaN
                trade_downside = np.NaN
                trade_upside_i = np.NaN
                trade_downside_i = np.NaN
                
                entry_price = src_idx(open_p, bar_index+1)
                entry_price_arr[start_bar] = entry_price
                entry_day_arr[start_bar] = src_idx(day,bar_index+1)
                entry_price_i = src_idx(open_i, bar_index+1)
                
                trade_return1 =  src_idx(open_p, bar_index+2) - entry_price
                trade_return5 =  src_idx(open_p, bar_index+6) - entry_price
                trade_return10 = src_idx(open_p, bar_index+11) - entry_price
                trade_return15 = src_idx(open_p, bar_index+16) - entry_price
                
                total_return1 +=  trade_return1
                total_return5 +=  trade_return5
                total_return10 += trade_return10
                total_return15 += trade_return15
                    
                return_arr1[start_bar] = trade_return1
                return_arr5[start_bar] = trade_return5
                return_arr10[start_bar] = trade_return10
                return_arr15[start_bar] = trade_return15
                
                num_win1  += 1 if trade_return1  > 0 else 0
                num_win5  += 1 if trade_return5  > 0 else 0
                num_win10 += 1 if trade_return10 > 0 else 0
                num_win15 += 1 if trade_return15 > 0 else 0

                winrate_arr1[start_bar] = num_win1 / num_trades * 100
                winrate_arr5[start_bar] = num_win5 / num_trades * 100
                winrate_arr10[start_bar] = num_win10 / num_trades * 100
                winrate_arr15[start_bar] = num_win15 / num_trades * 100
                
                avg_returns_arr1[start_bar] = total_return1 / num_trades
                avg_returns_arr5[start_bar] = total_return5 / num_trades
                avg_returns_arr10[start_bar] = total_return10 / num_trades
                avg_returns_arr15[start_bar] = total_return15 / num_trades
                
             
            if is_trading:
                if (bar_index - start_bar >= 1):
                    runup = (high_p[bar_index] - entry_price) / entry_price * 100
                    drawdown = (low_p[bar_index] - entry_price) / entry_price * 100
                    trade_upside = runup if np.isnan(trade_upside) else max(trade_upside, runup)
                    trade_downside = drawdown if np.isnan(trade_downside) else min(trade_downside, drawdown)
                    
                    runup_i = (high_i[bar_index] - entry_price_i) / entry_price_i * 100
                    drawdown_i = (low_i[bar_index] - entry_price_i) / entry_price_i * 100
                    trade_upside_i = runup_i if np.isnan(trade_upside_i) else max(trade_upside_i, runup_i)
                    trade_downside_i = drawdown_i if np.isnan(trade_downside_i) else min(trade_downside_i, drawdown_i)
                    
                trading_status[bar_index] = 1
                
            # match_exit = (
            #     (bar_index - start_bar >= 1) & (exit_signals[bar_index])
            # ) | (
            #     (bar_index - start_bar) == holding_periods
            # )
            
            exit_by_signals = (bar_index - start_bar >= 1) & (exit_signals[bar_index])
            exit_by_periods = use_holding_periods and ((bar_index - start_bar) == holding_periods)
            
            exit_by_profloss = False
            if (bar_index - start_bar >= 1):
                pc = Utils.calc_percentage_change(entry_price, close_p[bar_index])
                exit_by_profloss = use_takeprofit_cutloss and ((pc >= profit_thres) or (pc <= -loss_thres))

            match_exit = exit_by_signals or exit_by_periods or exit_by_profloss
            
            if is_trading and match_exit:
                
                is_trading = False
                
                close_price = src_idx(open_p, bar_index+1)
                if not np.isnan(close_price):
                    close_price_i = src_idx(open_i, bar_index+1)

                    price_change = (close_price - entry_price) * direction
                    price_change_i = (close_price_i - entry_price_i) * direction
                    
                    trade_return = price_change / entry_price * 100
                    trade_return_i = price_change_i / entry_price_i * 100

                    
                    if trade_return > 0:
                        num_win = num_win + 1
                        total_profit = total_profit + price_change
    
                    if trade_return < 0:
                        total_loss = total_loss + price_change
                        
                    if trade_return_i > 0:
                        num_win_i = num_win_i + 1
                        total_profit_i = total_profit_i + price_change_i
    
                    if trade_return_i < 0:
                        total_loss_i = total_loss_i + price_change_i
                    
                    total_return = total_return + trade_return
                    total_return_i = total_return_i + trade_return_i
                    
                    trade_upside =  max(trade_upside, (close_price - entry_price) / entry_price * 100)
                    trade_downside =  min(trade_downside,  (close_price - entry_price) / entry_price * 100)
                    trade_upside_i =  max(trade_upside_i, (close_price_i - entry_price_i) / entry_price_i * 100)
                    trade_downside_i =  min(trade_downside_i,  (close_price_i - entry_price_i) / entry_price_i * 100)
                
                    max_runup = trade_upside if np.isnan(max_runup) else max(max_runup, trade_upside)
                    max_drawdown = trade_downside if np.isnan(max_drawdown) else min(max_drawdown, trade_downside)
                    max_runup_i = trade_upside_i if np.isnan(max_runup_i) else max(max_runup_i, trade_upside_i)
                    max_drawdown_i = trade_downside_i if np.isnan(max_drawdown_i) else min(max_drawdown_i, trade_downside_i)

                    winrate_arr[start_bar] = num_win / num_trades * 100
                    avg_returns_arr[start_bar] = total_return / num_trades
                    
                    total_upside += trade_upside
                    total_downside += trade_downside
                    upside_arr[start_bar] = trade_upside
                    downside_arr[start_bar] = trade_downside
                    max_runup_arr[start_bar] = max_runup
                    max_drawdown_arr[start_bar] = max_drawdown
                    avg_upside_arr[start_bar] = total_upside / num_trades
                    avg_downside_arr[start_bar] = total_downside / num_trades
                    
                    profit_factor_arr[start_bar] = (total_profit / (total_loss * -1) if total_loss != 0 else np.NaN)

                    exit_price_arr[start_bar] = close_price
                    exit_day_arr[start_bar] = src_idx(day, bar_index+1)
                    return_arr[start_bar] = trade_return
                    price_change_arr[start_bar] = price_change
                    
                    return_arr_i[start_bar] = trade_return_i
                    winrate_arr_i[start_bar] = num_win_i / num_trades * 100
                    avg_returns_arr_i[start_bar] = total_return_i / num_trades
                    
                    total_upside_i += trade_upside_i
                    total_downside_i += trade_downside_i
                    upside_arr_i[start_bar] = trade_upside_i
                    downside_arr_i[start_bar] = trade_downside_i
                    max_runup_arr_i[start_bar] = max_runup_i
                    max_drawdown_arr_i[start_bar] = max_drawdown_i
                    avg_upside_arr_i[start_bar] = total_upside_i / num_trades
                    avg_downside_arr_i[start_bar] = total_downside_i / num_trades
                    
                    profit_factor_arr_i[start_bar] = (total_profit_i / (total_loss_i * -1) if total_loss_i != 0 else np.NaN)                
                
        return  (
            entry_price_arr,
            entry_day_arr,
            direction,
            exit_signals,
            exit_price_arr,
            exit_day_arr,
            trading_status,
            num_trades_arr,
            return_arr,
            return_arr1,
            return_arr5,
            return_arr10,
            return_arr15,
            winrate_arr,
            winrate_arr1,
            winrate_arr5,
            winrate_arr10,
            winrate_arr15,
            avg_returns_arr,
            avg_returns_arr1,
            avg_returns_arr5,
            avg_returns_arr10,
            avg_returns_arr15,
            profit_factor_arr,
            upside_arr,
            downside_arr,
            avg_upside_arr,
            avg_downside_arr,
            max_drawdown_arr,
            max_runup_arr,
            max_drawdown_arr,
            is_entry_arr,
            price_change_arr,
            winrate_arr_i,
            avg_returns_arr_i,
            profit_factor_arr_i,
            return_arr_i,
            upside_arr_i,
            downside_arr_i,
            max_runup_arr_i,
            max_drawdown_arr_i,
            avg_upside_arr_i,
            avg_downside_arr_i,
        )

    def run(
        self, 
        signals = None,
        trade_direction="Long", 
        compute_start_time="2018_01_01", 
        use_shift=False,
        n_shift=15, 
        holding_periods=8
    ):
        df = self.df
        func = self.func
        params = self.params
        name = self.name
        
        """Run simmulation"""
        if "return" not in df.columns:
            if Globs.verbosity == 1:
                logging.warning(
                    "Input dataframe was not prepared for backtesting,It will be prepared now!"
                )
            df = self.prepare_data_for_backtesting(
                df,
                trade_direction=trade_direction,
                holding_periods=holding_periods,
            )

        if signals is None:
            signals = func(df, params, use_shift = use_shift, n_shift=n_shift)
        
        if signals is None:
            df["signal"] = Utils.new_1val_series(True, df) 
        else:
            df["signal"] = signals
        
        df["signal"] = np.where((df["day"] < compute_start_time) | (df["signal"].isna()), False, df["signal"]).astype(bool)
        df["name"] = name

        # Compute trade stats using Numba
        price_changes = df["priceChange"].values
        returns = df["return"].values
        returns1 = df["return1"].values
        returns5 = df["return5"].values
        returns10 = df["return10"].values
        returns15 = df["return15"].values
        signals = df["signal"].values
        upsides = df["upside"].values
        downsides = df["downside"].values
        (
            trading_status,
            num_trades_arr,
            winrate_arr,
            winrate1_arr,
            winrate5_arr,
            winrate10_arr,
            winrate15_arr,
            avg_returns_arr,
            avg_returns1_arr,
            avg_returns5_arr,
            avg_returns10_arr,
            avg_returns15_arr,
            avg_upside_arr,
            avg_downside_arr,
            max_runup_arr,
            max_drawdown_arr,
            profit_factor_arr,
            is_entry_arr,
        ) = self.compute_trade_stats(
            price_changes, 
            returns, 
            returns1, 
            returns5, 
            returns10, 
            returns15, 
            signals, 
            upsides, 
            downsides, 
            holding_periods
        )

        # Update DataFrame
        df["numTrade"] = num_trades_arr
        df["winrate"] = winrate_arr
        df["winrateT1"] = winrate1_arr
        df["winrateT5"] = winrate5_arr
        df["winrateT10"] = winrate10_arr
        df["winrateT15"] = winrate15_arr
        df["avgReturn"] = avg_returns_arr
        df["avgReturnT1"] = avg_returns1_arr
        df["avgReturnT5"] = avg_returns5_arr
        df["avgReturnT10"] = avg_returns10_arr
        df["avgReturnT15"] = avg_returns15_arr
        df["avgUpside"] = avg_upside_arr
        df["avgDownside"] = avg_downside_arr
        df["maxRunup"] = max_runup_arr
        df["maxDrawdown"] = max_drawdown_arr
        df["profitFactor"] = profit_factor_arr
        df["isEntry"] = is_entry_arr
        df["isTrading"] = trading_status

        df["matched"] = np.where(df["signal"], 1, np.NaN)
        df[
            [
                "numTrade",
                "winrate",
                "winrateT1",
                "winrateT5",
                "winrateT10",
                "winrateT15",
                "profitFactor",
                "avgReturn",
                "avgReturnT1",
                "avgReturnT5",
                "avgReturnT10",
                "avgReturnT15",
                "avgUpside",
                "avgDownside",
                "maxRunup",
                "maxDrawdown"
            ]
        ] = df[
            [
                "numTrade",
                "winrate",
                "winrateT1",
                "winrateT5",
                "winrateT10",
                "winrateT15",
                "profitFactor",
                "avgReturn",
                "avgReturnT1",
                "avgReturnT5",
                "avgReturnT10",
                "avgReturnT15",
                "avgUpside",
                "avgDownside",
                "maxRunup",
                "maxDrawdown"
            ]
        ].ffill().fillna(0)
        self.df = df

        result = df[
            [
                "name",
                "numTrade",
                "winrate",
                "profitFactor",
                "avgReturn",
                "avgUpside",
                "avgDownside",
                "maxRunup",
                "maxDrawdown",
                "isEntry",
                "matched",
                "isTrading",
            ]
        ].iloc[-1]
        result.name = self.name
        self.result = result
   
    def run2(self, 
            signals = None,
            trade_direction="Long", 
            use_shift = False,
            n_shift = 15,
            compute_start_time="2018_01_01", 
            use_holding_periods=True,
            holding_periods=60,
            use_takeprofit_cutloss=False,
            profit_thres=5,
            loss_thres=5,
            *args, **kwargs
            ):
        """
        Backtest có xét thêm params: take profit and cutloss (khác so với run và run3); 
        Gọi 'compute_trade_stats2'
        
        Purpose:
        --------
        This function runs a full backtest simulation based on entry and exit signals, evaluating 
        the performance of trades using metrics such as win rate, average returns, upside/downside, and profit factor. 
        It processes input data and merges additional market data to produce comprehensive backtest results.
        """
        df = self.df
        func = self.func
        params = self.params
        exit_params = self.exit_params
        name = self.name
        
        print(len(df))
        
        def test_params():
            trade_direction="Long"
            use_shift = False,
            n_shift = 15,
            use_holding_periods = False
            holding_periods=15
            profit_thres=5,
            loss_thres=5
            use_takeprofit_cutloss = True
            compute_start_time="2018_01_01"
            df = df_raw.copy()
            func = Conds.compute_any_conds
            
            params = {
            'bbpctb': {
                'src_name': 'close',
                'length': 20,
                'mult': 2,
                'use_flag': True,
                'use_range': False,
                'low_range': 80,
                'high_range': 100,
                'use_cross': True,
                'direction': 'crossover',
                'cross_line': 'Upper band'
                }
            }
            
            exit_params = params.pop('exit_cond') if 'exit_cond' in params else {} 
            name = 'HPG'
        
# %%
        
        if signals is None:
                signals = func(df, params, use_shift = use_shift, n_shift=n_shift)
        
        df['signal'] = Utils.new_1val_series(True, df) if signals is None else signals
        df["signal"] = np.where((df["day"] < compute_start_time) | (df["signal"].isna()), False, df["signal"]).astype(bool)
  
        exit_signals = Conds.compute_any_conds(df, exit_params)
        df["exitSignal"] = Utils.new_1val_series(False, df) if exit_signals is None else exit_signals
        
        df["name"] = name
        
        dfi = glob_obj.get_one_stock_data('VNINDEX')
        dfi = dfi[['day', 'open', 'high', 'low', 'close']]
        dfi.columns = ['day', 'iopen', 'ihigh', 'ilow', 'iclose']
        df = pd.merge(df, dfi, how='left', on='day')
        
        df['iclose'] = df['iclose'].ffill()
        df['iopen'] = df['iopen'].fillna(df['iclose'])
        df['ihigh'] = df['ihigh'].fillna(df['iclose'])
        df['ilow'] = df['ilow'].fillna(df['iclose'])

        
        (
            entry_price_arr,
            entry_day_arr,
            direction,
            exit_signals,
            exit_price_arr,
            exit_day_arr,
            trading_status,
            num_trades_arr,
            return_arr,
            return_arr1,
            return_arr5,
            return_arr10,
            return_arr15,
            winrate_arr,
            winrate_arr1,
            winrate_arr5,
            winrate_arr10,
            winrate_arr15,
            avg_returns_arr,
            avg_returns_arr1,
            avg_returns_arr5,
            avg_returns_arr10,
            avg_returns_arr15,
            profit_factor_arr,
            upside_arr,
            downside_arr,
            avg_upside_arr,
            avg_downside_arr,
            max_drawdown_arr,
            max_runup_arr,
            max_drawdown_arr,
            is_entry_arr,
            price_change_arr,
            winrate_arr_i,
            avg_returns_arr_i,
            profit_factor_arr_i,
            return_arr_i,
            upside_arr_i,
            downside_arr_i,
            max_runup_arr_i,
            max_drawdown_arr_i,
            avg_upside_arr_i,
            avg_downside_arr_i
        )=Simulator.compute_trade_stats2(
            day=df['day'].astype(int).values,
            open_p=df['open'].values, 
            high_p=df['high'].values,
            low_p=df['low'].values,
            close_p=df['close'].values,
            open_i=df['iopen'].values,
            high_i=df['ihigh'].values,
            low_i=df['ilow'].values,
            close_i=df['iclose'].values,
            entry_signals=df['signal'].values,
            exit_signals=df['exitSignal'].values,
            direction = 1 if trade_direction == 'Long' else -1,
            use_holding_periods=use_holding_periods,
            holding_periods=holding_periods,
            use_takeprofit_cutloss=use_takeprofit_cutloss,
            profit_thres=profit_thres,
            loss_thres=loss_thres
        )
        def strday(x):  
            y = str(x)
            return y[:4] +'_'+ y[4:6] +'_'+ y[6:8]

        df["entryPrice"] =   entry_price_arr
        df["entryDay"] =     entry_day_arr

        df['entryDay'] = np.where(
            df['entryDay'].notna(),
            df['entryDay'].apply(lambda x: strday(x)),
            np.NaN
        )

        
        df["entryType"] =    direction
        df["exitSignal"] =   exit_signals
        df["exitPrice"] =    exit_price_arr
        df["exitDay"] =      exit_day_arr
        df['exitDay'] = np.where(
            df['exitDay'].notna(),
            df['exitDay'].apply(lambda x: strday(x)),
            np.NaN
        )
        
        df["isTrading"] =    trading_status
        df["numTrade"] =     num_trades_arr
        df["return"] =       return_arr
        df["return1"] =      return_arr1
        df["return5"] =      return_arr5
        df["return10"] =     return_arr10
        df["return15"] =     return_arr15
        df["winrate"] =      winrate_arr
        df["winrateT1"] =    winrate_arr1
        df["winrateT5"] =    winrate_arr5
        df["winrateT10"] =   winrate_arr10
        df["winrateT15"] =   winrate_arr15
        df["avgReturn"] =    avg_returns_arr
        df["avgReturnT1"] =  avg_returns_arr1
        df["avgReturnT5"] =  avg_returns_arr5
        df["avgReturnT10"]=  avg_returns_arr10
        df["avgReturnT15"]=  avg_returns_arr15
        df["profitFactor"]=  profit_factor_arr
        df["upside"] =       upside_arr
        df["downside"] =     downside_arr
        df["avgUpside"] =    avg_upside_arr
        df["avgDownside"] =  avg_downside_arr
        df["maxDrawdown"] =  max_drawdown_arr
        df["maxRunup"]    =  max_runup_arr
        df["maxDrawdown"] =  max_drawdown_arr
        df["isEntry"]     =  is_entry_arr
        df["priceChange"] =  price_change_arr
        df["matched"] =      np.where(df["signal"], 1, np.NaN)
        df["winrateIdx"] = winrate_arr_i
        df["avgReturnsIdx"] = avg_returns_arr_i
        df["profitFactorIdx"] = profit_factor_arr_i
        df["returnIdx"] = return_arr_i
        df["upsideIdx"] = upside_arr_i
        df["downsideIdx"] = downside_arr_i
        df["maxRunupIdx"] = max_runup_arr_i
        df["maxDrawdownIdx"] = max_drawdown_arr_i
        df["avgUpsideIdx"] = avg_upside_arr_i
        df["avgDownsideIdx"] = avg_downside_arr_i
        
        df[
            [
                "numTrade",
                "winrate",
                "winrateT1",
                "winrateT5",
                "winrateT10",
                "winrateT15",
                "profitFactor",
                "avgReturn",
                "avgReturnT1",
                "avgReturnT5",
                "avgReturnT10",
                "avgReturnT15",
                "avgUpside",
                "avgDownside",
                "maxRunup",
                "maxDrawdown",
                "winrateIdx",
                "avgReturnsIdx",
                "profitFactorIdx",
                "maxRunupIdx",
                "maxDrawdownIdx",
                "avgUpsideIdx",
                "avgDownsideIdx",
            ]
        ] = df[
            [
                "numTrade",
                "winrate",
                "winrateT1",
                "winrateT5",
                "winrateT10",
                "winrateT15",
                "profitFactor",
                "avgReturn",
                "avgReturnT1",
                "avgReturnT5",
                "avgReturnT10",
                "avgReturnT15",
                "avgUpside",
                "avgDownside",
                "maxRunup",
                "maxDrawdown",
                "winrateIdx",
                "avgReturnsIdx",
                "profitFactorIdx",
                "maxRunupIdx",
                "maxDrawdownIdx",
                "avgUpsideIdx",
                "avgDownsideIdx",
            ]
        ].ffill().fillna(0)


        result = df[
            [
                "name",
                "numTrade",
                "winrate",
                "profitFactor",
                "avgReturn",
                "avgUpside",
                "avgDownside",
                "maxRunup",
                "maxDrawdown",
                "isEntry",
                "matched",
                "isTrading",
                "winrateIdx",
                "avgReturnsIdx",
                "profitFactorIdx",
                "maxRunupIdx",
                "maxDrawdownIdx",
                "avgUpsideIdx",
                "avgDownsideIdx",
            ]
        ].iloc[-1]

        
        self.df = df    
        self.result = result

    @staticmethod
    @njit
    def compute_trade_stats(
        price_changes, 
        returns, 
        returns1,
        returns5,
        returns10,
        returns15,
        signals, 
        upsides, 
        downsides, 
        holding_periods
    ):
        """Computing trade stats, using numba"""
        num_trades = 0
        num_win = 0
        num_win1 = 0
        num_win5 = 0
        num_win10 = 0
        num_win15 = 0

        
        
        num_loss = 0
        total_profit = 0.0
        total_loss = 0.0
        total_return = 0.0
        total_return1 = 0.0
        total_return5 = 0.0
        total_return10 = 0.0
        total_return15 = 0.0
        
        
        total_upside = 0.0
        total_downside = 0.0
        
        max_runup = np.NaN
        max_drawdown = np.NaN

        is_trading = False
        entry_idx = 0
        trading_status = np.full(len(signals), np.NaN)
        num_trades_arr = np.full(len(signals), np.NaN)
        
        winrate_arr = np.full(len(signals), np.NaN)
        winrate1_arr = np.full(len(signals), np.NaN)
        winrate5_arr = np.full(len(signals), np.NaN)
        winrate10_arr = np.full(len(signals), np.NaN)
        winrate15_arr = np.full(len(signals), np.NaN)
        
        avg_returns_arr = np.full(len(signals), np.NaN)
        avg_returns1_arr = np.full(len(signals), np.NaN)
        avg_returns5_arr = np.full(len(signals), np.NaN)
        avg_returns10_arr = np.full(len(signals), np.NaN)
        avg_returns15_arr = np.full(len(signals), np.NaN)
        
        profit_factor_arr = np.full(len(signals), np.NaN)
        is_entry_arr = np.full(len(signals), np.NaN)

        avg_upside_arr = np.full(len(signals), np.NaN)
        avg_downside_arr = np.full(len(signals), np.NaN)
        
        max_runup_arr = np.full(len(signals), np.NaN)
        max_drawdown_arr = np.full(len(signals), np.NaN)

        for index, signal in enumerate(signals):
            if signal and not is_trading:
                is_trading = True
                entry_idx = index

                trade_return = returns[index]

                if not np.isnan(trade_return):
                    num_trades += 1
                    trade_profit = price_changes[index]

                    total_return += trade_return
                    total_return1 += returns1[index]
                    total_return5 += returns5[index]
                    total_return10 += returns10[index]
                    total_return15 += returns15[index]
                    
                    total_upside += upsides[index]
                    total_downside += downsides[index]
                    
                    # max runup, max drawdown
                    if np.isnan(max_runup):
                        max_runup = upsides[index]
                    else:
                        max_runup = max(max_runup, upsides[index])
                        
                    if np.isnan(max_drawdown):
                        max_drawdown = downsides[index]
                    else:
                        max_drawdown = min(max_drawdown, downsides[index])
                    
                    if trade_profit > 0:
                        num_win += 1
                        total_profit += trade_profit
                    else:
                        num_loss += 1
                        total_loss += trade_profit
                        
                    num_win1  += 1 if returns1[index]  > 0 else 0
                    num_win5  += 1 if returns5[index]  > 0 else 0
                    num_win10 += 1 if returns10[index] > 0 else 0
                    num_win15 += 1 if returns15[index] > 0 else 0

                    num_trades_arr[index] = num_trades
                    
                    winrate_arr[index] = num_win / num_trades * 100
                    winrate1_arr[index] = num_win1 / num_trades * 100
                    winrate5_arr[index] = num_win5 / num_trades * 100
                    winrate10_arr[index] = num_win10 / num_trades * 100
                    winrate15_arr[index] = num_win15 / num_trades * 100
                    
                    avg_returns_arr[index] = total_return / num_trades
                    avg_returns1_arr[index] = total_return1 / num_trades
                    avg_returns5_arr[index] = total_return5 / num_trades
                    avg_returns10_arr[index] = total_return10 / num_trades
                    avg_returns15_arr[index] = total_return15 / num_trades
                    
                    avg_upside_arr[index] = total_upside / num_trades
                    avg_downside_arr[index] = total_downside / num_trades
                    
                    max_runup_arr[index] = max_runup
                    max_drawdown_arr[index] = max_drawdown
                    
                    profit_factor_arr[index] = (
                        total_profit / (total_loss * -1) if total_loss != 0 else np.NaN
                    )

                is_entry_arr[index] = 1

            if is_trading and (index - entry_idx == holding_periods):
                is_trading = False

            if is_trading:
                trading_status[index] = 1

        return (
            trading_status,
            num_trades_arr,
            winrate_arr,
            winrate1_arr,
            winrate5_arr,
            winrate10_arr,
            winrate15_arr,
            avg_returns_arr,
            avg_returns1_arr,
            avg_returns5_arr,
            avg_returns10_arr,
            avg_returns15_arr,
            avg_upside_arr,
            avg_downside_arr,
            max_runup_arr,
            max_drawdown_arr,
            profit_factor_arr,
            is_entry_arr,
        )

    def run(
        self, 
        signals = None,
        trade_direction="Long", 
        compute_start_time="2018_01_01", 
        use_shift=False,
        n_shift=15, 
        holding_periods=8,
        *args, **kwargs
    ):
        df = self.df
        func = self.func
        params = self.params
        name = self.name
        
        """Run simmulation"""
        if "return" not in df.columns:
            if Globs.verbosity == 1:
                logging.warning(
                    "Input dataframe was not prepared for backtesting,It will be prepared now!"
                )
            df = self.prepare_data_for_backtesting(
                df,
                trade_direction=trade_direction,
                holding_periods=holding_periods,
            )
        if signals is None:
            signals = func(df, params, use_shift = use_shift, n_shift=n_shift)
        
        if signals is None:
            df["signal"] = Utils.new_1val_series(True, df) 
        else:
            df["signal"] = signals
        
        df["signal"] = np.where((df["day"] < compute_start_time) | (df["signal"].isna()), False, df["signal"]).astype(bool)
        df["name"] = name

        # Compute trade stats using Numba
        price_changes = df["priceChange"].values
        returns = df["return"].values
        returns1 = df["return1"].values
        returns5 = df["return5"].values
        returns10 = df["return10"].values
        returns15 = df["return15"].values
        signals = df["signal"].values
        upsides = df["upside"].values
        downsides = df["downside"].values
        (
            trading_status,
            num_trades_arr,
            winrate_arr,
            winrate1_arr,
            winrate5_arr,
            winrate10_arr,
            winrate15_arr,
            avg_returns_arr,
            avg_returns1_arr,
            avg_returns5_arr,
            avg_returns10_arr,
            avg_returns15_arr,
            avg_upside_arr,
            avg_downside_arr,
            max_runup_arr,
            max_drawdown_arr,
            profit_factor_arr,
            is_entry_arr,
        ) = self.compute_trade_stats(
            price_changes, 
            returns, 
            returns1, 
            returns5, 
            returns10, 
            returns15, 
            signals, 
            upsides, 
            downsides, 
            holding_periods
        )

        # Update DataFrame
        df["numTrade"] = num_trades_arr
        df["winrate"] = winrate_arr
        df["winrateT1"] = winrate1_arr
        df["winrateT5"] = winrate5_arr
        df["winrateT10"] = winrate10_arr
        df["winrateT15"] = winrate15_arr
        df["avgReturn"] = avg_returns_arr
        df["avgReturnT1"] = avg_returns1_arr
        df["avgReturnT5"] = avg_returns5_arr
        df["avgReturnT10"] = avg_returns10_arr
        df["avgReturnT15"] = avg_returns15_arr
        df["avgUpside"] = avg_upside_arr
        df["avgDownside"] = avg_downside_arr
        df["maxRunup"] = max_runup_arr
        df["maxDrawdown"] = max_drawdown_arr
        df["profitFactor"] = profit_factor_arr
        df["isEntry"] = is_entry_arr
        df["isTrading"] = trading_status

        df["matched"] = np.where(df["signal"], 1, np.NaN)
        df[
            [
                "numTrade",
                "winrate",
                "winrateT1",
                "winrateT5",
                "winrateT10",
                "winrateT15",
                "profitFactor",
                "avgReturn",
                "avgReturnT1",
                "avgReturnT5",
                "avgReturnT10",
                "avgReturnT15",
                "avgUpside",
                "avgDownside",
                "maxRunup",
                "maxDrawdown"
            ]
        ] = df[
            [
                "numTrade",
                "winrate",
                "winrateT1",
                "winrateT5",
                "winrateT10",
                "winrateT15",
                "profitFactor",
                "avgReturn",
                "avgReturnT1",
                "avgReturnT5",
                "avgReturnT10",
                "avgReturnT15",
                "avgUpside",
                "avgDownside",
                "maxRunup",
                "maxDrawdown"
            ]
        ].ffill().fillna(0)
        self.df = df

        result = df[
            [
                "name",
                "numTrade",
                "winrate",
                "profitFactor",
                "avgReturn",
                "avgUpside",
                "avgDownside",
                "maxRunup",
                "maxDrawdown",
                "isEntry",
                "matched",
                "isTrading",
            ]
        ].iloc[-1]
        result.name = self.name
        self.result = result

  

    def run3(
        self, signals = None, trade_direction="Long", compute_start_time="2018_01_01", use_shift=False,
        n_shift=15, holding_periods=8,
        *args, **kwargs
    ):
        df = self.df
        func = self.func
        params = self.params
        name = self.name
        
        def test():
            stock = 'HPG'
            df_stock: pd.DataFrame = Adapters.load_stocks_data_from_plasma()
            df = df_stock.pivot(index = 'day', columns = 'stock')\
                .xs(stock, axis=1, level='stock')\
                .reset_index(drop=False)
            df['stock'] = stock
            df = Simulator.prepare_data_for_backtesting(
                df,
                trade_direction=trade_direction,
                holding_periods=holding_periods,
            )
            func = Conds.compute_any_conds

        """
        Backtest ko có params: chốt lời/cắt lỗ, tính toán trực tiếp trên Dataframe
        without using a separate calculation function like `compute_trade_stats()` like 'run'.
        """

        if "return" not in df.columns:
            if Globs.verbosity == 1:
                logging.warning(
                    "Input dataframe was not prepared for backtesting,It will be prepared now!"
                )
            df = self.prepare_data_for_backtesting(
                df,
                trade_direction=trade_direction,
                holding_periods=holding_periods,
            )
        
        if signals is None:
            signals = func(df, params, use_shift = use_shift, n_shift=n_shift)
        
        if signals is None:
            df["signal"] = Utils.new_1val_series(True, df) 
        else:
            df["signal"] = signals
            
        df["signal"] = np.where((df["day"] < compute_start_time) | (df["signal"].isna()), False, df["signal"]).astype(bool)
        # df["signal"].iloc[-(holding_periods+1):] = False

        df["name"] = name

        # Update DataFrame
        df['isEntry'] = df['signal'].copy()
        df["isEntry"].iloc[-(holding_periods+1):] = False

        df['isTrading'] = df['isEntry'].rolling(holding_periods).sum().fillna(0).astype(bool)
        df['numTrade'] = df['isEntry'].cumsum()
        
        df['is_win'] = (df['isEntry']) & (df['return']>0)
        df['is_win1'] = (df['isEntry'] ) & (df['return1']>0)
        df['is_win5'] = (df['isEntry'] ) & (df['return5']>0)
        df['is_win10'] = (df['isEntry']) & (df['return10']>0)
        df['is_win15'] = (df['isEntry']) & (df['return15']>0)

        df['winrate'] = df['is_win'].cumsum() / df['numTrade'] * 100
        df['winrateT1'] = df['is_win1'].cumsum() / df['numTrade'] * 100
        df['winrateT5'] = df['is_win5'].cumsum() / df['numTrade'] * 100
        df['winrateT10'] = df['is_win10'].cumsum() / df['numTrade'] * 100
        df['winrateT15'] = df['is_win15'].cumsum() / df['numTrade'] * 100

        df['avgReturn'] = (df['isEntry'] * df['return']).cumsum() / df['numTrade']
        df['avgReturnT1'] = (df['isEntry'] * df['return1']).cumsum() / df['numTrade']
        df['avgReturnT5'] = (df['isEntry'] * df['return5']).cumsum() / df['numTrade']
        df['avgReturnT10'] = (df['isEntry'] * df['return10']).cumsum() / df['numTrade']
        df['avgReturnT15'] = (df['isEntry'] * df['return15']).cumsum() / df['numTrade']

        df['avgUpside'] = (df['isEntry'] * df['upside']).cumsum() / df['numTrade']
        df['avgDownside'] = (df['isEntry'] * df['downside']).cumsum() / df['numTrade']

        df['75thRunup'] = (df['isEntry'] * df['upside']).expanding().apply(lambda x: np.percentile(x[(x != 0) & ~np.isnan(x)] , 75) if (x[(x != 0) & ~np.isnan(x)].size > 0) else np.nan, raw=True)
      
        df['75thDrawdown'] = (df['isEntry'] * df['downside']).expanding().apply(lambda x: np.percentile(x[(x != 0) & ~np.isnan(x)], 25) if (x[(x != 0) & ~np.isnan(x)].size > 0) else np.nan, raw=True)


        df['maxRunup'] = (df['isEntry'] * df['upside']).cummax()
        df['maxDrawdown'] = (df['isEntry'] * df['downside']).cummin()

        df['profit'] = df['isEntry'] * df['return'].clip(lower = 0)
        df['loss'] = df['isEntry'] * df['return'].clip(upper = 0) * (-1)

        df['profitFactor'] = np.where(df['loss']!=0, df['profit'].cumsum() / df['loss'].cumsum(), np.nan)
        
        df = df.drop(['is_win', 'is_win1', 'is_win5', 'is_win10', 'is_win15', 'profit', 'loss' ], axis = 1)
        df['isEntry'] = np.where(df['isEntry'], 1, np.nan)

        df["matched"] = np.where(df["signal"], 1, np.NaN)
        df[
            [
                "numTrade",
                "winrate",
                "winrateT1",
                "winrateT5",
                "winrateT10",
                "winrateT15",
                "profitFactor",
                "avgReturn",
                "avgReturnT1",
                "avgReturnT5",
                "avgReturnT10",
                "avgReturnT15",
                "avgUpside",
                "avgDownside",
                "maxRunup",
                "maxDrawdown",
                "75thRunup",
                "75thDrawdown"
            ]
        ] = df[
            [
                "numTrade",
                "winrate",
                "winrateT1",
                "winrateT5",
                "winrateT10",
                "winrateT15",
                "profitFactor",
                "avgReturn",
                "avgReturnT1",
                "avgReturnT5",
                "avgReturnT10",
                "avgReturnT15",
                "avgUpside",
                "avgDownside",
                "maxRunup",
                "maxDrawdown",
                "75thRunup",
                "75thDrawdown"
            ]
        ].ffill().fillna(0)
        self.df = df

        result = df[
            [
                "name",
                "numTrade",
                "winrate",
                "profitFactor",
                "avgReturn",
                "avgUpside",
                "avgDownside",
                "maxRunup",
                "maxDrawdown",
                "isEntry",
                "matched",
                "isTrading",
            ]
        ].iloc[-1]
        result.name = self.name
        self.result = result


class Scanner:
    """Scanner"""

    @staticmethod
    def scan_multiple_stocks(
        params, 
        stocks=None,
        trade_direction="Long", 
        use_shift=False,
        n_shift=15,
        holding_periods=60):
        """Backtest on multiple stocks
        Sử dụng `scan_one_stock`, không xét params cắt lỗ/chốt lời (khác `scan_multiple_stocks3`), 
        cũng không có điều kiện lọc theo ngày (khác `scan_multiple_stocks_v4`).
        """
        def test():
            params = (
                {
                    'price_change':{
                        'lower_thres':5,
                        'use_flag': True
                    }
                }
            )
            stocks=None
            trade_direction="Long" 
            use_shift=False
            n_shift=15
            holding_periods=60


        """Backtest on multiple stocks"""
        from danglib.pylabview.celery_worker import scan_one_stock, clean_redis
        from tqdm import tqdm

        clean_redis()
        
        sum_ls = []
        trades_ls = []
        err_stocks = {}
        task_dic = {}

        if stocks is None:
            stocks = glob_obj.stocks
        
        params = Globs.old_saved_adapters(params)
        
        for stock in tqdm(stocks):

            task = scan_one_stock.delay(
                params=params,
                stock = stock,
                name=stock,
                trade_direction=trade_direction,
                use_shift=use_shift,
                n_shift=n_shift,
                holding_periods=holding_periods
            )
            
            task_dic[stock] = task
            
        # while any(t.status!='SUCCESS' for t in task_dic.values()):
        #     pass

        while any(t.status not in ['SUCCESS', 'FAILURE'] for t in task_dic.values()):
            if any(t.status == 'FAILURE' for t in task_dic.values()):
                # Xử lý khi có task bị lỗi
                raise CeleryTaskError("Có task bị lỗi. Thoát vòng lặp: ", [i for i, j in task_dic.items() if j.status == 'FAILURE'])
            pass  #

        for k, v in task_dic.items():
            bt = v.result
            if bt is not None:
                sum_ls.append(bt.result)
                trades_ls.append(bt.df)
                
            
        res_df = pd.DataFrame(sum_ls)
        res_df = res_df[
            [
                "name",
                "numTrade",
                "winrate",
                "profitFactor",
                "avgReturn",
                "avgUpside",
                "avgDownside",
            ]
        ].round(2).fillna(-999)
        res_df["beta_group"] = res_df["name"].map(glob_obj.dic_groups).fillna('unknown')

        trades_df = pd.concat(trades_ls)    
        trades_df['beta_group'] = trades_df['stock'].map(glob_obj.dic_groups).fillna('unknown')
        return res_df, trades_df
    
    @staticmethod
    def scan_multiple_stocks_loop(
        func, 
        params, 
        stocks=None,
        trade_direction="Long", 
        use_shift=False,
        n_shift=15,
        holding_periods=60):
        """
        Backtest on multiple stocks, cho phép dùng hàm điều kiện tùy chọn.

        Cho phép sử dụng hàm điều kiện tùy chọn, phù hợp cho các chiến lược phức tạp hơn (không áp dụng trong các hàm khác).
        """
        def test():
            func = Conds.compute_any_conds
            params = {
                'net_income': {
                    'use_flag': True,
                    'percentage': 2,
                }
            }
            trade_direction = "Long"
            stocks = ['HPG', 'SSI']
            use_shift = False
            n_shift =  50
            holding_periods = 15
        
        df_all_stocks = Adapters.load_stocks_data_from_plasma()
        if stocks is None:
            stocks = glob_obj.stocks
        else:
            df_all_stocks= df_all_stocks[df_all_stocks['stock'].isin(stocks)]
        
        sum_ls = []
        trades_ls = []
        
        params = Globs.old_saved_adapters(params)
        
        for stock, df_stock in df_all_stocks.groupby("stock"):
            df_stock = df_stock.reset_index(drop=True)

            
            bt = Simulator(
                func,
                df_ohlcv=df_stock,
                params=params,
                name=stock,
            )
            try:
                bt.run(
                    trade_direction=trade_direction, 
                    use_shift=use_shift,
                    n_shift=n_shift,
                    holding_periods=holding_periods
                )
            except Exception as e:
                print(f"scan error: {e}")
            
            
            sum_ls.append(bt.result)
            trades_ls.append(bt.df)
            
            
        res_df = pd.DataFrame(sum_ls)
        res_df = res_df[
            [
                "name",
                "numTrade",
                "winrate",
                "profitFactor",
                "avgReturn",
                "avgUpside",
                "avgDownside",
            ]
        ].round(2).fillna(-999)
        res_df["beta_group"] = res_df["name"].map(glob_obj.dic_groups).fillna('unknown')

        trades_df = pd.concat(trades_ls)    
        trades_df['beta_group'] = trades_df['stock'].map(glob_obj.dic_groups)
        return res_df, trades_df
    
    @staticmethod
    def scan_multiple_stocks_v2(
        params, 
        stocks=None,
        trade_direction="Long", 
        use_shift=False,
        n_shift=15,
        holding_periods=60):
        """
        Backtest nhóm cổ phiếu cụ thể 

        Sử dụng 'scan_one_stock', Chỉ định nhóm cổ phiếu cụ thể (khác `scan_multiple_stocks`, `v3`, `v4`).
        """

        from danglib.pylabview.celery_worker import scan_one_stock, clean_redis
        from tqdm import tqdm


        stocks = ['VN30', 'VNDIAMOND', 'VNINDEX', 'VNMID', 'VNSML']
        
        clean_redis()
        
        sum_ls = []
        trades_ls = []
        task_dic = {}
        
        params = Globs.old_saved_adapters(params)
        
        for stock in tqdm(stocks):

            task = scan_one_stock.delay(
                params=params,
                stock=stock,
                name=stock,
                trade_direction=trade_direction,
                use_shift=use_shift,
                n_shift=n_shift,
                holding_periods=holding_periods
            )
            
            task_dic[stock] = task
            
        # while any(t.status!='SUCCESS' for t in task_dic.values()):
        #     pass

        while any(t.status not in ['SUCCESS', 'FAILURE'] for t in task_dic.values()):
            if any(t.status == 'FAILURE' for t in task_dic.values()):
                # Xử lý khi có task bị lỗi
                raise CeleryTaskError("Có task bị lỗi. Thoát vòng lặp: ", [i for i, j in task_dic.items() if j.status == 'FAILURE'])
            pass  #

        for k, v in task_dic.items():
            bt = v.result
            if bt is not None:
                sum_ls.append(bt.result)
                trades_ls.append(bt.df)
                
            
        res_df = pd.DataFrame(sum_ls)
        res_df = res_df[
            [
                "name",
                "numTrade",
                "winrate",
                "profitFactor",
                "avgReturn",
                "avgUpside",
                "avgDownside",
            ]
        ].round(2).fillna(-999)
        res_df["beta_group"] = 'Super High Beta'

        trades_df = pd.concat(trades_ls)    
        trades_df['beta_group'] = 'Super High Beta'
        return res_df, trades_df
    
    @staticmethod
    def scan_multiple_stocks3(
        params, 
        stocks=None, 
        trade_direction="Long",
        use_shift=False,
        n_shift=15,
        use_holding_periods=True,
        holding_periods=60,
        use_takeprofit_cutloss=False,
        profit_thres=5,
        loss_thres=5
        ):
        """
        Backtest xét thêm params cắt lỗ/chốt lời tùy chỉnh cho cổ phiếu.
   
        - Hỗ trợ cấu hình cắt lỗ/chốt lời tùy chỉnh cao nhất (khác với các phiên bản còn lại).
        """
        # exit_params = params.pop('exit_cond') if 'exit_cond' in params else {}
        from danglib.pylabview.celery_worker import scan_one_stock_v2, clean_redis
        from tqdm import tqdm

    
        if stocks is None:
            stocks = glob_obj.stocks
        
        clean_redis()
        
        sum_ls = []
        trades_ls = []
        err_stocks = {}
        task_dic = {}
        
        params = Globs.old_saved_adapters(params)
        
        for stock in tqdm(stocks):

            task = scan_one_stock_v2.delay(
                params=params,
                stock = stock,
                name=stock,
                trade_direction=trade_direction,
                use_shift=use_shift,
                n_shift=n_shift,
                use_holding_periods=use_holding_periods,
                holding_periods=holding_periods,
                use_takeprofit_cutloss=use_takeprofit_cutloss,
                profit_thres=profit_thres,
                loss_thres=loss_thres
            )
            
            task_dic[stock] = task
            
        while any(t.status not in ['SUCCESS', 'FAILURE'] for t in task_dic.values()):
            if any(t.status == 'FAILURE' for t in task_dic.values()):
                # Xử lý khi có task bị lỗi
                raise CeleryTaskError("Có task bị lỗi. Thoát vòng lặp: ", [i for i, j in task_dic.items() if j.status == 'FAILURE'])
            pass  #

        for k, v in task_dic.items():
            bt = v.result
            if bt is not None:
                sum_ls.append(bt.result)
                trades_ls.append(bt.df)
                
            
        res_df = pd.DataFrame(sum_ls)
        res_df = res_df[
            [
                "name",
                "numTrade",
                "winrate",
                "profitFactor",
                "avgReturn",
                "avgUpside",
                "avgDownside",
                "maxRunup",
                "maxDrawdown",
                "winrateIdx",
                "avgReturnsIdx",
                "profitFactorIdx",
                "maxRunupIdx",
                "maxDrawdownIdx",
                "avgUpsideIdx",
                "avgDownsideIdx"
            ]
        ].round(2).fillna(-999)
        res_df["beta_group"] = res_df["name"].map(glob_obj.dic_groups).fillna('unknown')

        trades_df = pd.concat(trades_ls)    
        trades_df['beta_group'] = trades_df['stock'].map(glob_obj.dic_groups).fillna('unknown')
# %%
        return res_df, trades_df

    @staticmethod
    def scan_multiple_stocks_v3(
        params, 
        stocks=None,
        trade_direction="Long", 
        use_shift=False,
        n_shift=15,
        holding_periods=60):
        """
        Backtest không hỗ trợ cắt lỗ/chốt lời

        - Sử dụng `scan_one_stock_v3` không có cắt lỗ/chốt lời (khác với `v2` và `3`).
        """

        from danglib.pylabview.celery_worker import scan_one_stock_v3, clean_redis
        from tqdm import tqdm

        def test():
            params = {'stock_scanner': {'trade_direction': 'Long', 'use_shift': False, 'n_shift': 15, 'holding_periods': 15}, 'price_change': {'use_flag': False, 'periods': 1, 'direction': 'increase', 'lower_thres': 5, 'upper_thres': 100}, 'price_comp_ma': {'use_flag': True, 'ma_len1': 5, 'ma_len2': 15, 'ma_type': 'EMA', 'ma_dir': 'crossover'}, 'price_gap': {'use_flag': False, 'gap_dir': 'Use Gap Up'}, 'price_change_vs_hl': {'use_flag': False, 'direction': 'Increase', 'nbars': 10, 'low_range': 5, 'high_range': 100}, 'price_highest_lowest': {'use_flag': False, 'method': 'Highest', 'num_bars': 10}, 'consecutive_conditional_bars': {'use_flag': False, 'src1_name': 'close', 'src2_name': 'close', 'direction': 'Increase', 'num_bars': 5, 'num_matched': 4}, 'vol_comp_ma': {'use_flag': False, 'n_bars': 1, 'ma_len': 20, 'comp_ma_dir': 'higher', 'comp_ma_perc': 20}, 'vol_percentile': {'use_flag': False, 'ma_length': 10, 'ranking_window': 128, 'low_range': 0, 'high_range': 100}, 'consecutive_squeezes': {'bb_length': 20, 'length_kc': 20, 'mult_kc': 1.5, 'use_true_range': True, 'use_flag': False, 'num_bars': 1}, 
                      'ursi': {'length': 14, 'smo_type1': 'RMA', 'smooth': 14, 'smo_type2': 'EMA', 'use_flag': True, 'use_vs_signal': True, 'direction': 'crossover', 'use_range': False, 'lower_thres': 0, 'upper_thres': 0}, 'macd': {'r2_period': 20, 'fast': 10, 'slow': 20, 'signal_length': 9, 'use_flag': False, 'use_vs_signal': False, 'direction': 'crossover', 'use_range': False, 'lower_thres': 0, 'upper_thres': 0}, 'bbwp': {'src_name': 'close', 'basic_type': 'SMA', 'bbwp_len': 13, 'bbwp_lkbk': 128, 'use_flag': False, 'use_low_thres': False, 'low_thres': 20, 'use_high_thres': False, 'high_thres': 80}, 'bbpctb': {'src_name': 'close', 'length': 20, 'mult': 2, 'use_flag': False, 'use_range': False, 'low_range': 80, 'high_range': 100, 'use_cross': False, 'direction': 'crossover', 'cross_line': 'Upper band'}, 'net_income': {'use_flag': False, 'calc_type': 'QoQ', 'roll_len': 2, 'direction': 'positive', 'percentage': 0}, 'index_cond': {'price_comp_ma': {'use_flag': False, 'ma_len1': 5, 'ma_len2': 15, 'ma_type': 'EMA', 'ma_dir': 'crossover'}, 'ursi': {'length': 14, 'smo_type1': 'RMA', 'smooth': 14, 'smo_type2': 'EMA', 'use_flag': False, 'use_vs_signal': False, 'direction': 'crossover', 'use_range': False, 'lower_thres': 0, 'upper_thres': 0}, 'bbwp': {'src_name': 'close', 'basic_type': 'SMA', 'bbwp_len': 13, 'bbwp_lkbk': 128, 'use_flag': False, 'use_low_thres': False, 'low_thres': 20, 'use_high_thres': False, 'high_thres': 80}, 'bbpctb': {'src_name': 'close', 'length': 20, 'mult': 2, 'use_flag': False, 'use_range': False, 'low_range': 80, 'high_range': 100, 'use_cross': False, 'direction': 'crossover', 'cross_line': 'Upper band'}}, 'lookback_cond': {'n_bars': 5, 'price_change': {'use_flag': False, 'periods': 1, 'direction': 'increase', 'lower_thres': 0, 'upper_thres': 100}, 'price_comp_ma': {'use_flag': False, 'ma_len1': 5, 'ma_len2': 15, 'ma_type': 'EMA', 'ma_dir': 'crossover'}, 'price_gap': {'use_flag': False, 'gap_dir': 'Use Gap Up'}, 'price_change_vs_hl': {'use_flag': False, 'direction': 'Increase', 'nbars': 10, 'low_range': 5, 'high_range': 100}, 'price_highest_lowest': {'use_flag': False, 'method': 'Highest', 'num_bars': 10}, 'consecutive_conditional_bars': {'use_flag': False, 'src1_name': 'close', 'src2_name': 'close', 'direction': 'Increase', 'num_bars': 5, 'num_matched': 4}, 'vol_comp_ma': {'use_flag': False, 'n_bars': 1, 'ma_len': 20, 'comp_ma_dir': 'higher', 'comp_ma_perc': 20}, 'vol_percentile': {'use_flag': False, 'ma_length': 10, 'ranking_window': 128, 'low_range': 0, 'high_range': 100}, 'consecutive_squeezes': {'bb_length': 20, 'length_kc': 20, 'mult_kc': 1.5, 'use_true_range': True, 'use_flag': False, 'num_bars': 1}, 'ursi': {'length': 14, 'smo_type1': 'RMA', 'smooth': 14, 'smo_type2': 'EMA', 'use_flag': False, 'use_vs_signal': False, 'direction': 'crossover', 'use_range': False, 'lower_thres': 0, 'upper_thres': 0}, 'macd': {'r2_period': 20, 'fast': 10, 'slow': 20, 'signal_length': 9, 'use_flag': False, 'use_vs_signal': False, 'direction': 'crossover', 'use_range': False, 'lower_thres': 0, 'upper_thres': 0}, 'bbwp': {'src_name': 'close', 'basic_type': 'SMA', 'bbwp_len': 13, 'bbwp_lkbk': 128, 'use_flag': False, 'use_low_thres': False, 'low_thres': 20, 'use_high_thres': False, 'high_thres': 80}, 'bbpctb': {'src_name': 'close', 'length': 20, 'mult': 2, 'use_flag': False, 'use_range': False, 'low_range': 80, 'high_range': 100, 'use_cross': False, 'direction': 'crossover', 'cross_line': 'Upper band'}}, 'default_selector_stocks': []}
            # stock_scanner_params = params['stock_scanner']
            trade_direction =  'Long',
            use_shift =  False,
            n_shift =  15,
            holding_periods =  15
            stocks = glob_obj.stocks
        
        
        if stocks is None:
            stocks = glob_obj.stocks
        
        clean_redis()
        
        sum_ls = []
        trades_ls = []
        err_stocks = {}
        task_dic = {}
        
        params = Globs.old_saved_adapters(params)
        
        for stock in tqdm(stocks):

            task = scan_one_stock_v3.delay(
                params=params,
                stock = stock,
                name=stock,
                trade_direction=trade_direction,
                use_shift=use_shift,
                n_shift=n_shift,
                holding_periods=holding_periods
            )
            
            task_dic[stock] = task
            
        # while any(t.status!='SUCCESS' for t in task_dic.values()):
        #     pass

        while any(t.status not in ['SUCCESS', 'FAILURE'] for t in task_dic.values()):
            if any(t.status == 'FAILURE' for t in task_dic.values()):
                # Xử lý khi có task bị lỗi
                raise CeleryTaskError("Có task bị lỗi. Thoát vòng lặp: ", [i for i, j in task_dic.items() if j.status == 'FAILURE'])
            pass  #

        for k, v in task_dic.items():
            bt = v.result
            if bt is not None:
                sum_ls.append(bt.result)
                trades_ls.append(bt.df)
                
            
        res_df = pd.DataFrame(sum_ls)
        res_df = res_df[
            [
                "name",
                "numTrade",
                "winrate",
                "profitFactor",
                "avgReturn",
                "avgUpside",
                "avgDownside",
            ]
        ].round(2).fillna(-999)
        res_df["beta_group"] = res_df["name"].map(glob_obj.dic_groups).fillna('unknown')

        trades_df = pd.concat(trades_ls)    
        trades_df['beta_group'] = trades_df['stock'].map(glob_obj.dic_groups).fillna('unknown')

        return res_df, trades_df

    @staticmethod
    def scan_multiple_stocks_v4(
        params, 
        stocks=None,
        trade_direction="Long", 
        use_shift=False,
        n_shift=15,
        holding_periods=60):
        """
        Backtest với điều kiện lọc dữ liệu theo ngày.
        Gọi `scan_one_stock_v4`, giúp phân tích chi tiết theo ngày (khác tất cả các phiên bản khác).
        Trong scan_one_stock_v4, dữ liệu cổ phiếu được định dạng lại để phân tích chi tiết theo từng ngày. 
        Việc lọc và xét điều kiện theo ngày được thực hiện trong nội dung của scan_one_stock_v4, giúp hàm scan_multiple_stocks_v4 hỗ trợ phân tích backtest chi tiết hơn, khác biệt với các phiên bản khác.
        """
        def test():
            stocks = None
            use_shift=False,
            n_shift =15
            holding_periods = 60

        from danglib.pylabview.celery_worker import scan_one_stock_v4, clean_redis
        from tqdm import tqdm
        if stocks is None:
            stocks = glob_obj.stocks
            
        clean_redis()
        
        sum_ls = []
        trades_ls = []
        err_stocks = {}
        task_dic = {}
        
        params = Globs.old_saved_adapters(params)
        
        for stock in tqdm(stocks):  
            
            task = scan_one_stock_v4.delay(
                params=params,
                stock = stock,
                name=stock,
                trade_direction=trade_direction,
                use_shift=use_shift,
                n_shift=n_shift,
                holding_periods=holding_periods
            )
            
            task_dic[stock] = task
            

        while any(t.status not in ['SUCCESS', 'FAILURE'] for t in task_dic.values()):
            if any(t.status == 'FAILURE' for t in task_dic.values()):
                # Xử lý khi có task bị lỗi
                raise CeleryTaskError("Có task bị lỗi. Thoát vòng lặp: ", [i for i, j in task_dic.items() if j.status == 'FAILURE'])
            pass  #

        for k, v in task_dic.items():
            bt = v.result
            if bt is not None:
                sum_ls.append(bt.result)
                trades_ls.append(bt.df)
                
            
        res_df = pd.DataFrame(sum_ls)
        res_df = res_df[
            [
                "name",
                "numTrade",
                "winrate",
                "profitFactor",
                "avgReturn",
                "avgUpside",
                "avgDownside",
            ]
        ].round(2).fillna(-999)
        res_df["beta_group"] = res_df["name"].map(glob_obj.dic_groups).fillna('unknown')

        trades_df = pd.concat(trades_ls)    
        trades_df['beta_group'] = trades_df['stock'].map(glob_obj.dic_groups).fillna('unknown')
        return res_df, trades_df
    
    @staticmethod
    def compute_signals_for_multiple_stocks(params, stocks = None):

        def test():
            params = {
                'price_change': {
                    'use_flag': True
                }
            }
            stocks = None
            stocks = glob_obj.stocks


        from danglib.pylabview.celery_worker import compute_one_stock_signals, clean_redis
        clean_redis()

        if stocks is None:
            stocks = glob_obj.stocks

        ls = []
        task_dic = {}
        
        for stock in stocks:  
            task = compute_one_stock_signals.delay(
                params = params,
                stock = stock
            )
            task_dic[stock] = task

        while any(t.status not in ['SUCCESS', 'FAILURE'] for t in task_dic.values()):
            if any(t.status == 'FAILURE' for t in task_dic.values()):
                # Xử lý khi có task bị lỗi
                raise CeleryTaskError("Có task bị lỗi. Thoát vòng lặp: ", [i for i, j in task_dic.items() if j.status == 'FAILURE'])
            pass  #

        for k, v in task_dic.items():
            res = v.result
            if res is not None:
                ls.append(res)

        df_signals: pd.DataFrame = pd.concat(ls, axis =1, join='outer')

        return df_signals
    
    @staticmethod
    def scan_multiple_stocks_with_signals(
        df_signals: pd.DataFrame,
        calc_type: int = 1,
        trade_direction: str = "Long",
        use_shift: bool = False,
        n_shift: int = 15,
        use_holding_periods: bool = True,
        holding_periods: int = 60,
        use_takeprofit_cutloss: bool = False,
        profit_thres: float = 5,
        loss_thres: float = 5
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Backtest multiple stocks using pre-computed signals.

        Args:
            df_signals: DataFrame containing signals for multiple stocks (columns are stock symbols)
            calc_type: Type of calculation to use (1, 2, or 3)
            trade_direction: Trading direction ('Long' or 'Short')
            use_shift: Whether to shift signals
            n_shift: Number of periods to shift
            use_holding_periods: Whether to use holding periods
            holding_periods: Number of periods to hold position
            use_takeprofit_cutloss: Whether to use take profit/cut loss
            profit_thres: Take profit threshold percentage
            loss_thres: Stop loss threshold percentage

        Returns:
            tuple containing:
                - Summary DataFrame with performance metrics for each stock
                - Detailed trades DataFrame for all stocks
        """
        def test():
            df_signals = Scanner.compute_signals_for_multiple_stocks(
            params={
                    'price_change': {
                        'periods': 1,
                        'direction': 'increase',
                        'lower_thres': 3,
                        'upper_thres': 100,
                        'use_flag': True
                    }
                },
                stocks=glob_obj.index_names[:3]  # Test with first 3 indexes
            )
            calc_type: int = 1
            trade_direction: str = "Long"
            use_shift: bool = False
            n_shift: int = 15
            use_holding_periods: bool = True
            holding_periods: int = 60
            use_takeprofit_cutloss: bool = False
            profit_thres: float = 5
            loss_thres: float = 5
        

        from danglib.pylabview.celery_worker import scan_one_stock_v5, clean_redis
            
        clean_redis()
        
        sum_ls = []
        trades_ls = []
        task_dic = {}

        trading_params = {
            "trade_direction": trade_direction,
            "use_shift": use_shift, 
            "n_shift": n_shift,
            "use_holding_periods": use_holding_periods,
            "holding_periods": holding_periods,
            "use_takeprofit_cutloss": use_takeprofit_cutloss,
            "profit_thres": profit_thres,
            "loss_thres": loss_thres
        }
        
        # Start tasks for each stock
        for stock in df_signals.columns:  
            task = scan_one_stock_v5.delay(
                signals=df_signals[stock].dropna().reset_index(drop=True),
                calc_type=calc_type,
                stock=stock,
                **trading_params
            )
            task_dic[stock] = task

        # Monitor tasks and handle failures
        while any(t.status not in ['SUCCESS', 'FAILURE'] for t in task_dic.values()):
            failed_tasks = [i for i, j in task_dic.items() if j.status == 'FAILURE']
            if failed_tasks:
                raise CeleryTaskError(f"Tasks failed for stocks: {failed_tasks}")

        # Process results
        for stock, task in task_dic.items():
            bt = task.result
            if bt is not None:
                sum_ls.append(bt.result)
                trades_ls.append(bt.df)
                
        # Create summary DataFrame
        res_df = pd.DataFrame(sum_ls)
        summary_cols = [
            "name",
            "numTrade",
            "winrate",
            "profitFactor",
            "avgReturn",
            "avgUpside",
            "avgDownside",
        ]
        res_df = res_df[summary_cols].round(2).fillna(-999)
        res_df["beta_group"] = res_df["name"].map(glob_obj.dic_groups).fillna('unknown')

        # Create trades DataFrame
        trades_df = pd.concat(trades_ls)    
        trades_df['beta_group'] = trades_df['stock'].map(glob_obj.dic_groups).fillna('unknown')
        
        return res_df, trades_df
    

    

    
    

glob_obj = Globs()

glob_obj.load_all_data2()
try:
    glob_obj.get_one_stock_data("HPG")
except:
    glob_obj.gen_stocks_data()

def test():

    params = (
        {   
            'price_change':{
                'lower_thres':3,
                'use_flag': True
            }

        }
    )

    res, df = Scanner.scan_multiple_stocks_v4(
            params=params,
            trade_direction='Long', 
            # stocks =['HPG'],
            # use_holding_periods=True,
            holding_periods=15,
            # use_takeprofit_cutloss=False,
            # profit_thres=5,
            # loss_thres=5
        )
    
    from danglib.pylabview.celery_worker import clean_redis
    clean_redis()






if __name__ == "__main__":
    
    
    # glob_obj.gen_stocks_data()
    
    df_raw = glob_obj.get_one_stock_data("AGG")
    # try:
    #     # Tạo một đối tượng ArgumentParser
    #     parser = argparse.ArgumentParser(description="A program to demonstrate command line arguments.")

    #     # Thêm các đối số
    #     parser.add_argument('--updatedata', action='store_true', help="Update stocks data for Pylabview")

    #     args = parser.parse_args()
        
    #     if args.updatedata:
    #         glob_obj.gen_stocks_data(send_viber=True)
    # except:
    #     pass
    
    # show_ram_usage_mb()
    # glob_obj.load_stocks_data()
    # df = glob_obj.load_stocks_data()
# celery -A celery_worker worker --concurrency=10 --loglevel=INFO -n celery_worker@pylabview 

    def test_loop():
        Conds.price_change
        Conds.Fa.net_income
        func = Conds.compute_any_conds
        params = {
            "lookback_cond":{
                'n_bars': 15,
                'net_income': {
                    'use_flag': True,
                    'percentage': 2
                }
            }
        }
        trade_direction = "Long"
        stocks = ['HPG', 'SSI']
        use_shift = True
        n_shift =  50
        holding_periods = 15
        
        df_res, df_trades = Scanner.scan_multiple_stocks_loop(
            func=func,
            params=params,
            stocks=stocks,
            trade_direction=trade_direction,
            use_shift=use_shift,
            n_shift=n_shift,
            holding_periods=holding_periods
        )
        df_res
        
        # glob_obj.gen_stocks_data()


