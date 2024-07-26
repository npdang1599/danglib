"""Import functions"""

import pprint
import logging
from danglib.pylabview.core_lib import pd, np, Ta, Utils, Adapters, Math, Fns
from numba import njit
from pymongo import MongoClient
import logging
import warnings
import argparse
from danglib.chatbots.viberbot import F5bot

pd.options.mode.chained_assignment = None


# Tắt tất cả các FutureWarning
warnings.simplefilter(action='ignore', category=FutureWarning)


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
            "bbpctb": Conds.bbpctb,
            "net_income": Conds.Fa.net_income,
            "revenue": Conds.Fa.revenue,
            "inventory": Conds.Fa.inventory,
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
        }
        self.sectors = {}

    def __getattribute__(self, name):
        if name == "df_stocks":
            if(self._df_stocks is None):
                self.load_stocks_data()
            return self._df_stocks
        else:
            return super().__getattribute__(name)
    
    def load_stocks_data(self):
        """run to load stocks data"""
        self._df_stocks = Adapters.load_stocks_data_from_main_db()

    def load_stocks_data_pickle(self):
        """Load data from pickle"""
        self._df_stocks = Adapters.load_stocks_data_from_pickle()

    def get_one_stock_data(self, stock):
        """Get one stock data from df_stocks"""
        df: pd.DataFrame = self.df_stocks[self.df_stocks["stock"] == stock].copy()
        df = df.reset_index(drop=True)
        return df
    
    def load_vnindex(self):
        self.df_vnindex: pd.DataFrame = Adapters.get_stock_from_vnstock(
            "VNINDEX", from_day=self.data_from_day
        )

    def gen_stocks_data(self, fn=None, collection=None, send_viber=False):
        try:
            if fn is None:
                fn = Fns.pickle_stocks_data
            stocks = self.stocks
            Adapters.prepare_stocks_data(stocks, fn=fn, db_collection=collection, to_pickle=False, to_mongo=True)

            df = Adapters.load_stocks_data_from_main_db()
            latest_day = df['day'].max()
            
            msg = f"Stocks data update for Pylabview has been completed! Latest day: {latest_day}"

        except Exception as e:
            msg = f"Failed to update data for Pylabview, error: {e}"
            logging.error(msg)

        if send_viber:
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
        sectors = {
            'Steel': ['HPG', 'NKG', 'HSG'],
            'Hog': ['DBC','BAF','MML'],
            'Fish': ['VHC','ANV','IDI'], 
            'Textile': ['TNG','MSH','STK'], 
            'Homebuilder':['PTB','VCS','GDT'], 
            'Chemicals1': ['DGC','DPM','DCM'],
            'Chemicals2': ['CSV','LAS','BFC']
        }

        data_list = []
        for sector, stocks in sectors.items():
            for stock in stocks:
                data_list.append({'stock': stock, 'sector': sector})

        df = pd.DataFrame(data_list)

        for stock in self.stocks:
            if stock not in df['stock']:
                data_list.append({'stock':stock, 'sector':'other'})

        df = pd.DataFrame(data_list)

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
        def range_cond(line, lower_thres, upper_thres, use_flag):
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
        def hlest_cond(df: pd.DataFrame, src_name: str, hl_options:str='highest', n_bars: int = 10, use_flag: bool = False):
            if use_flag:
                src = df[src_name]
                func = Ta.is_highest if hl_options == 'highest' else Ta.is_lowest
                cond = func(src, n_bars)
                return cond

            return None
        
        @staticmethod
        def std_cond(df: pd.DataFrame, src_name: str, n_bars: int = 10, mult: float = 2.0, position:str = 'higher', use_flag:bool = False):
            if use_flag:
                src = df[src_name]
                std = Ta.stdev(src, n_bars)
                mean = Ta.sma(src, n_bars)
                upper = mean + std * mult
                cond = src > upper if position == 'higher' else src < upper
                return cond
            
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
        high_range: float = 100
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

            return Utils.in_range(pct_change, low_range, high_range, equal=True)

        return None

    @staticmethod
    def price_gap(df: pd.DataFrame, gap_dir="Use Gap Up", use_flag: bool = True):
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
        use_flag: bool = False,
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
            # Calculating squeeze
            sqz_on, _, _ = Ta.squeeze(
                df, src_name, bb_length, length_kc, mult_kc, use_true_range
            )

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
                return df['cond']
            
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
                return df['cond']
            
            return None

        @staticmethod
        def PE(
            df: pd.DataFrame,
            use_flag = False,
            lowrange = 0,
            highrange = 10
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
            use_flag: bool = False
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
            percentage: float = 0
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
            # trend_calc_type: str= "QoQ",
            # trend_rolling_len: int = 1, 
            # trend_direction: str = "increase",
            # trend_n_quarters: float = 3,
            # trend_growth: str = 'acceleration', # deceleration
            # trend_use_flag: bool = False,

            # growth_use_flag: bool = False,
            # growth_calc_type: str = "QoQ",
            # growth_rolling_len: int = 1,
            # growth_direction: str = 'increase', 
            # growth_percentage: float = 0,
            
            hlest_use_flag: bool = False,
            hlest_direction: str = 'highest', 
            hlest_n_quarters: int = 3
        ):
            def test():
                df = glob_obj.get_one_stock_data("HPG")
                trend_calc_type: str= "QoQ"
                trend_rolling_len: int = 1 
                trend_direction: str = "increase"
                trend_n_quarters: float = 3
                trend_growth: str = 'acceleration' # deceleration
                trend_use_flag: bool = True

                growth_use_flag: bool = False
                growth_calc_type: str = "QoQ"
                growth_rolling_len: int = 1
                growth_direction: str = 'increase' 
                growth_percentage: float = 0
                
                hlest_use_flag: bool = False
                hlest_direction: str = 'highest' 
                hlest_n_quarters: int = 3
                
            df_inv = df.groupby('mapYQ').agg({'inventory':'last'})
            # trend_cond = Conds.Fa.trend(
            #     df_inv,
            #     src_name='inventory', 
            #     calc_type=trend_calc_type,
            #     rolling_len=trend_rolling_len,
            #     direction=trend_direction,
            #     number_of_quarters=trend_n_quarters,
            #     growth=trend_growth,
            #     use_flag=trend_use_flag
            # )
            
            # growth_cond = Conds.Fa.growth(
            #     df_inv,
            #     src_name='inventory',
            #     use_flag=growth_use_flag,
            #     calc_type=growth_calc_type,
            #     rolling_len=growth_rolling_len,
            #     direction=growth_direction,
            #     percentage=growth_percentage
            # )
            
            hlest_cond = Conds.Standards.hlest_cond(
                df_inv,
                src_name='inventory', 
                hl_options=hlest_direction,
                n_bars=hlest_n_quarters,
                use_flag=hlest_use_flag
            )
            
            # matched = Utils.combine_conditions([trend_cond, growth_cond, hlest_cond])
            
            if hlest_cond is not None:
                df_inv['cond'] = hlest_cond
                df['cond'] = df['mapYQ'].map(df_inv['cond'])
                return df['cond']
            
            return None
            
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
            ma_dir = 'crossover', # Direction
            hl_use_flag: bool = False, # Increase/Decrease over N bars
            hl_nbars = 5, # N Bars
            hl_dir = 'Increase', # Direction
            hl_lower = -999, # Lower
            hl_upper = 999 # Upper
        ):
            
            def test():
                steel_src='Ore 62'
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

            hl_cond = Conds.price_change_vs_hl(
                df_steel,
                src_name=steel_src,
                use_flag=hl_use_flag,
                direction=hl_dir,
                nbars=hl_nbars,
                low_range=hl_lower,
                high_range=hl_upper
            )

            steel_cond = Utils.combine_conditions([ma_cond, hl_cond])
            if steel_cond is not None:
                df_steel['cond'] = steel_cond
                return Utils.merge_condition(df, dict(zip(df_steel['day'], df_steel['cond'])))
            
            return None
        
        @staticmethod
        def coking_coal(
            df: pd.DataFrame, 
            show_src: bool = False,
            ma_use_flag: bool = False, # Coking Coal MA combination
            ma_type = 'EMA', # Method
            ma_len1 = 5, # MA1
            ma_len2 = 15, # MA2
            ma_dir = 'crossover', # Direction
            hl_use_flag: bool = False, # Increase/Decrease over N bars
            hl_nbars = 5, # N Bars
            hl_dir = 'Increase', # Direction
            hl_lower = -999, # Lower
            hl_upper = 999 # Upper
        ):
            return Conds.Sectors.steel_ma_and_hl(
                df = df,
                steel_src='Aus Coal',
                show_src=show_src,
                ma_use_flag = ma_use_flag,
                ma_type = ma_type,
                ma_len1 = ma_len1,
                ma_len2 = ma_len2,
                ma_dir = ma_dir,
                hl_use_flag = hl_use_flag,
                hl_nbars = hl_nbars,
                hl_dir = hl_dir,
                hl_lower = hl_lower,
                hl_upper = hl_upper,
            )
        
        @staticmethod
        def iron_ore(
            df: pd.DataFrame, 
            show_src: bool = False,
            ma_use_flag: bool = False, # Coking Coal MA combination
            ma_type = 'EMA', # Method
            ma_len1 = 5, # MA1
            ma_len2 = 15, # MA2
            ma_dir = 'crossover', # Direction
            hl_use_flag: bool = False, # Increase/Decrease over N bars
            hl_nbars = 5, # N Bars
            hl_dir = 'Increase', # Direction
            hl_lower = -999, # Lower
            hl_upper = 999 # Upper
        ):
            return Conds.Sectors.steel_ma_and_hl(
                df = df,
                steel_src='Ore 62',
                show_src=show_src,
                ma_use_flag = ma_use_flag,
                ma_type = ma_type,
                ma_len1 = ma_len1,
                ma_len2 = ma_len2,
                ma_dir = ma_dir,
                hl_use_flag = hl_use_flag,
                hl_nbars = hl_nbars,
                hl_dir = hl_dir,
                hl_lower = hl_lower,
                hl_upper = hl_upper,
            )
        
        @staticmethod
        def china_hrc(
            df: pd.DataFrame, 
            ma_use_flag: bool = False, # Coking Coal MA combination
            ma_type = 'EMA', # Method
            ma_len1 = 5, # MA1
            ma_len2 = 15, # MA2
            ma_dir = 'crossover', # Direction
            hl_use_flag: bool = False, # Increase/Decrease over N bars
            hl_nbars = 5, # N Bars
            hl_dir = 'Increase', # Direction
            hl_lower = -999, # Lower
            hl_upper = 999 # Upper
        ):
            return Conds.Sectors.steel_ma_and_hl(
                df = df,
                steel_src='China HRC',
                ma_use_flag = ma_use_flag,
                ma_type = ma_type,
                ma_len1 = ma_len1,
                ma_len2 = ma_len2,
                ma_dir = ma_dir,
                hl_use_flag = hl_use_flag,
                hl_nbars = hl_nbars,
                hl_dir = hl_dir,
                hl_lower = hl_lower,
                hl_upper = hl_upper,
            )
        
        @staticmethod
        def china_rebar(
            df: pd.DataFrame, 
            ma_use_flag: bool = False, # Coking Coal MA combination
            ma_type = 'EMA', # Method
            ma_len1 = 5, # MA1
            ma_len2 = 15, # MA2
            ma_dir = 'crossover', # Direction
            hl_use_flag: bool = False, # Increase/Decrease over N bars
            hl_nbars = 5, # N Bars
            hl_dir = 'Increase', # Direction
            hl_lower = -999, # Lower
            hl_upper = 999 # Upper
        ):
            return Conds.Sectors.steel_ma_and_hl(
                df = df,
                steel_src='China Long steel',
                ma_use_flag = ma_use_flag,
                ma_type = ma_type,
                ma_len1 = ma_len1,
                ma_len2 = ma_len2,
                ma_dir = ma_dir,
                hl_use_flag = hl_use_flag,
                hl_nbars = hl_nbars,
                hl_dir = hl_dir,
                hl_lower = hl_lower,
                hl_upper = hl_upper,
            )
        
        @staticmethod
        def steel_scrap(
            df: pd.DataFrame, 
            ma_use_flag: bool = False, # Coking Coal MA combination
            ma_type = 'EMA', # Method
            ma_len1 = 5, # MA1
            ma_len2 = 15, # MA2
            ma_dir = 'crossover', # Direction
            hl_use_flag: bool = False, # Increase/Decrease over N bars
            hl_nbars = 5, # N Bars
            hl_dir = 'Increase', # Direction
            hl_lower = -999, # Lower
            hl_upper = 999 # Upper
        ):
            return Conds.Sectors.steel_ma_and_hl(
                df = df,
                steel_src='Scrap',
                ma_use_flag = ma_use_flag,
                ma_type = ma_type,
                ma_len1 = ma_len1,
                ma_len2 = ma_len2,
                ma_dir = ma_dir,
                hl_use_flag = hl_use_flag,
                hl_nbars = hl_nbars,
                hl_dir = hl_dir,
                hl_lower = hl_lower,
                hl_upper = hl_upper,
            )
        
        @staticmethod
        def hpg_margin(
            df: pd.DataFrame, 
            ma_use_flag: bool = False, # Coking Coal MA combination
            ma_type = 'EMA', # Method
            ma_len1 = 5, # MA1
            ma_len2 = 15, # MA2
            ma_dir = 'crossover', # Direction
            hl_use_flag: bool = False, # Increase/Decrease over N bars
            hl_nbars = 5, # N Bars
            hl_dir = 'Increase', # Direction
            hl_lower = -999, # Lower
            hl_upper = 999 # Upper
        ):
            return Conds.Sectors.steel_ma_and_hl(
                df = df,
                steel_src='Margin',
                ma_use_flag = ma_use_flag,
                ma_type = ma_type,
                ma_len1 = ma_len1,
                ma_len2 = ma_len2,
                ma_dir = ma_dir,
                hl_use_flag = hl_use_flag,
                hl_nbars = hl_nbars,
                hl_dir = hl_dir,
                hl_lower = hl_lower,
                hl_upper = hl_upper,
            )

        # MARGIN_MA_useMA = input.bool(defval = false, title = 'MA combination', group = 'MARGIN MA conditions')
        # MARGIN_MA_MAType = input.string(defval = 'SMA', title = 'MA Method', inline = 'MA1', options = ['SMA'], group = 'MARGIN MA conditions')
        # MARGIN_MA_MALen1 = input.int(defval = 5, title = 'Line 1 MA Length', inline = 'MA2', group = 'MARGIN MA conditions')
        # MARGIN_MA_MALen2 = input.int(defval = 15, title = 'Line 2 MA Length', inline = 'MA3', group = 'MARGIN MA conditions')
        # MARGIN_MA_EntryCond = input.string(defval = 'crossover', title = 'Entry Conditions',options = ['crossover', 'crossunder', 'above', 'below'],inline = 'MA7', group = 'MARGIN MA conditions')
        # MARGIN_MA_useRange = input.bool(defval = false, title = 'Limit Range;', inline = 'MA4', group = 'MARGIN MA conditions')
        # MARGIN_MA_lower = input.float(defval = -99999999999, title = 'lower', inline = 'MA4', group = 'MARGIN MA conditions')
        # MARGIN_MA_upper = input.float(defval = 99999999999, title = 'upper', inline = 'MA4', group = 'MARGIN MA conditions')
        # MARGIN_hl_use_highest = input.bool(defval = false, title = 'Enter when margin is highest', group = 'MARGIN high/low conditions')
        # MARGIN_hl_use_lowest = input.bool(defval = false, title = 'Enter when margin is lowest', group = 'MARGIN high/low conditions')
        # MARGIN_hl_len = input.int(defval = 4, title = 'N Bar', group= 'MARGIN high/low conditions')
        # MARGIN_plot = input.bool(false, title = 'Plot Margin')
        
        # f_MARGIN_cond(margin) =>
        #     MARGIN_ma1 = dlib.f_ma(margin, MARGIN_MA_MALen1, MARGIN_MA_MAType)    
        #     MARGIN_ma2 = dlib.f_ma(margin, MARGIN_MA_MALen2, MARGIN_MA_MAType)    
        #     MARGIN_ma_cond = not MARGIN_MA_useMA or (dlib.f_define_2line_cond(MARGIN_ma1, MARGIN_ma2, MARGIN_MA_EntryCond) and 
        #                       (not MARGIN_MA_useRange or dlib.f_inrange(MARGIN_ma1, MARGIN_MA_lower, MARGIN_MA_upper) ))
        #     [isMax, isMin] = f_checkHLMargin(margin, MARGIN_hl_len)
        #     MARGIN_hl_cond = (not MARGIN_hl_use_highest or isMax) and (not MARGIN_hl_use_lowest or isMin)
        #     MRGINCond = MARGIN_ma_cond and MARGIN_hl_cond
        #     MRGINCond
        
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
        ):
            src = df['marginLending']
            ma1 = Ta.ma(src=src, length=ma_len1, ma_type=ma_type)
            ma2 = Ta.ma(src=src, length=ma_len2, ma_type=ma_type)
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
                    df_mg = df.groupby("mapYQ")[["marginLending"]].max()
                    func = Ta.is_highest if hl_options == 'highest' else Ta.is_lowest
                    df_mg['matched'] = func(df_mg['marginLending'], hl_nbars)
                    matched = df["mapYQ"].map(df_mg["matched"])
                    return matched

                return None
            
            cond = Utils.combine_conditions([ma_cond, hl_cond()])
            return cond
        
# DR_MA_useMA = input.bool(defval = false, title = 'MA combination', group = 'Deposit Rate MA conditions')
# DR_MA_MAType = input.string(defval = 'EMA', title = 'MA Method', inline = 'MA1', options = ['EMA', 'SMA'], group = 'Deposit Rate MA conditions')
# DR_MA_MALen1 = input.int(defval = 5, title = 'Line 1 MA Length', inline = 'MA2', group = 'Deposit Rate MA conditions')
# DR_MA_MALen2 = input.int(defval = 15, title = 'Line 2 MA Length', inline = 'MA3', group = 'Deposit Rate MA conditions')
# DR_MA_EntryCond = input.string(defval = 'crossover', title = 'Entry Conditions',options = ['crossover', 'crossunder', 'above', 'below'],inline = 'MA7', group = 'Deposit Rate MA conditions')
# DR_MA_useRange = input.bool(defval = false, title = 'Limit Range;', inline = 'MA4', group = 'Deposit Rate MA conditions')
# DR_MA_lower = input.float(defval = -99999999999, title = 'lower', inline = 'MA4', group = 'Deposit Rate MA conditions')
# DR_MA_upper = input.float(defval = 99999999999, title = 'upper', inline = 'MA4', group = 'Deposit Rate MA conditions')

# DR_sett_emaLen1 = input.int(5, title = 'EMA Len 1', group = 'Deposit Rate Percentile')
# DR_sett_emaLen2 = input.int(15, title = 'EMA Len 2', group ='Deposit Rate Percentile')
# DR_sett_percentilePeriod = input.int(128, title = 'Percentile Period', group = 'Deposit Rate Percentile')
# DR_input_usePerc = input.bool(defval = false, title = '', inline = 'vnivperc', group = 'Deposit Rate Percentile')
# DR_input_percentile = input.float(defval = 90, title = 'percentile', inline = 'vnivperc', group = 'Deposit Rate Percentile') / 100
# DR_input_direct = input.string('higher', options = ['higher', 'lower'], title = '' ,inline = 'vnivperc', group = 'Deposit Rate Percentile')
# DR_input_usePercDirect = input.bool(false, title = '', inline = 'vnivperc1', group = 'Deposit Rate Percentile')
# DR_input_percDirect = input.string('EMA1 > EMA2', options = ['EMA1 > EMA2', 'EMA2 > EMA1'], title = 'Compare Dir', inline = 'vnivperc1', group = 'Deposit Rate Percentile')

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
            pct_madir: str = 'EMA1 > EMA2' # 'EMA1 > EMA2' or 'EMA1 < EMA2'
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
            # DR_EMAV1 = ffill(ta.ema(DR, DR_sett_emaLen1))
            # DR_EMAV2 = ffill(ta.ema(DR, DR_sett_emaLen2))
            # DRVGap = DR_EMAV1 - DR_EMAV2
            # DRVGPer = rolling_rank(math.abs(DRVGap), DR_sett_percentilePeriod)

            # DR_percDirect = DR_input_percDirect == 'EMA1 > EMA2' ? DRVGap > 0 : DRVGap < 0 
            # DRVCond1 = not DR_input_usePerc or (f_compare_threshold(DRVGPer, DR_input_percentile, DR_input_direct) and (not DR_input_usePercDirect or DR_percDirect))

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
        
# f_check_macrodata(financial, usema, malen1, malen2, malines_pos, usechange, change_nperiod, change_dir, change_lrange, change_hrange, 
# usehlest, hlest, hlestperiods, usestd, stdpos, stdlen, stdmult) =>
# ma_cond = f_check_ma_cond(financial, usema, malen1, malen2, malines_pos)
# change_cond = f_check_change_cond(financial, usechange, change_nperiod, change_dir, change_lrange , change_hrange)
# hlest_cond = f_check_highest_lowest_cond(financial, hlestperiods, usehlest, hlest)
# std_cond = f_check_std_cond(financial, stdlen, stdmult, usestd, stdpos)

# ma_cond and change_cond and hlest_cond and std_cond
        @staticmethod
        def check_5sectors_macrodata(
            df: pd.DataFrame,
            sector:str,
            field: str,
            ma_use_flag: bool = False,
            ma_len1: int = 5,
            ma_len2: int = 15,
            ma_dir: str = 'crossover', 
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
            std_periods: int = 10
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
                
            df_sector = glob_obj.sectors[sector]['io_data'].copy()
            
            
            ma_cond = Conds.Standards.two_ma_lines(
                df=df_sector,
                src_name=field,
                ma_len1=ma_len1,
                ma_len2=ma_len2,
                ma_type='SMA',
                ma_dir=ma_dir,
                use_flag=ma_use_flag
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
            
            matched = Utils.combine_conditions([ma_cond, change_cond, hlest_cond, std_cond])

            if matched is not None:
                df_sector['cond'] = matched
                cond = Utils.merge_condition(df, dict(zip(df_sector['day'], df_sector['cond'])), fillna=False)
                cond = cond.ffill().fillna(False)
                return cond
            
            return  None
        
        # useFishPriceMA = input.bool(defval = false, title = 'Check Fish Price MA', group = '-----------Fish Macro Data')
        # FishPrice_MAlength1 = input.int(defval = 5, title = 'MA length 1',group = '-----------Fish Macro Data')
        # FishPrice_MAlength2 = input.int(defval = 15, title = 'MA length 2',group = '-----------Fish Macro Data')
        # FishPrice_MAlinepos = input.string(defval = 'above', title = 'Chọn vị trí giữa 2 đường MA', options = ['crossover','crossunder','above','below'],group = '-----------Fish Macro Data')
        # FishPrice_MAplot = input.bool(defval = false, title = 'Plot Fish Price', group = '-----------Fish Macro Data')
        
        # useFishPriceChange = input.bool(defval = false, title = 'Check Fish Price Change over N periods', group = '-----------Fish Macro Data')
        # FishPriceChange_NPeriods = input.int(defval = 5, title = 'N periods', group = '-----------Fish Macro Data')
        # FishPriceChange_dir      = input.string(defval = 'increase', options = ['increase', 'decrease'], title = 'Direction', group = '-----------Fish Macro Data')
        # FishPriceChange_lowrange = input.float(defval = 0, title = 'Low Range', inline = 'hog1', group = '-----------Fish Macro Data')
        # FishPriceChange_highrange = input.float(defval = 10, title = 'High Range', inline = 'hog1',group = '-----------Fish Macro Data' )

        # useFishPriceHLest = input.bool(defval = false, title = 'Check Fish Price Highest / Lowest', group = '-----------Fish Macro Data')
        # FishPriceHLest            = input.string(defval = 'highest', title = 'Use Highest/Lowest over x periods', options = ['highest','lowest'], inline = 'hog2', group = '-----------Fish Macro Data')
        # FishPriceXPeriods            = input.int(defval = 4, title = 'periods', inline = 'hog2', group = '-----------Fish Macro Data')

        # useFishPriceStd           = input.bool(defval = false, title = 'Check Fish Price Standard Deviation',  group = '-----------Fish Macro Data')
        # FishPriceStdPos           = input.string(defval = 'higher', title = '', options = ['higher','lower'], inline = 'hog3', group = '-----------Fish Macro Data')
        # FishPriceStdLength        = input.int(defval = 10, title ='Length', inline = 'hog3', group = '-----------Fish Macro Data')
        # FishPriceStdMult          = input.float(defval = 1.0, title = 'Multiplier',inline = 'hog3', group = '-----------Fish Macro Data' )

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
    def compute_any_conds(df: pd.DataFrame, functions_params_dic: dict, *args):
        """Compute and combine any conditions"""
        # df = df.copy()
        conds = []
        try:
            for func_name, params in functions_params_dic.items():
                if func_name == 'stock_scanner':
                    continue
                if(func_name not in glob_obj.function_map): 
                    continue
                func = glob_obj.function_map[func_name]
                cond = func(df, **params)
                if cond is not None:
                    conds.append(cond)
        except Exception as e:
            conds = []
            logging.error(f"function `compute_any_conds` error: {e}")

        if len(conds) > 0:
            return Utils.combine_conditions(conds)

        return None


    @staticmethod
    def index_cond(df: pd.DataFrame, **index_params):
        """Compute index condition"""
        df2 = glob_obj.df_vnindex.copy()

        index_cond = Conds.compute_any_conds(df2, index_params)
        if index_cond is not None:
            df2["index_cond"] = index_cond
            df = pd.merge(df, df2, how="left", on="day")
            df["index_cond"] = np.where(
                df["index_cond"].isna(), False, df["index_cond"]
            )
            df = df.infer_objects()
            return df["index_cond"]

        return None
    
    @staticmethod
    def index_cond2(df: pd.DataFrame, index_params):
        """Compute index condition"""
        df2 = glob_obj.df_vnindex.copy()

        index_cond = Conds.compute_any_conds(df2, index_params)
        if index_cond is not None:
            df2["index_cond"] = index_cond
            df = pd.merge(df, df2, how="left", on="day")
            df["index_cond"] = np.where(
                df["index_cond"].isna(), False, df["index_cond"]
            )
            df = df.infer_objects()
            return df["index_cond"]

        return None

    @staticmethod
    def lookback_cond(df: pd.DataFrame, n_bars: int, **lookback_params: dict):
        """Compute lookback condition"""
        lookback_cond = Conds.compute_any_conds(df, lookback_params)
        if lookback_cond is not None:
            return lookback_cond.rolling(n_bars, closed = 'left').max().astype(bool)

        return None

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
        """Prepare data"""
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
        # entry_condition = {
        #     'price_comp_ma':{
        #         "ma_len1":5,
        #         "ma_len2":15,
        #         "ma_type":"EMA",
        #         "ma_dir":"crossover",
        #         "use_flag": True,
        #     }
        # }

        # exit_condition = {
        #     'price_comp_ma':{
        #         "ma_len1":5,
        #         "ma_len2":15,
        #         "ma_type":"EMA",
        #         "ma_dir":"below",
        #         "use_flag": True,
        #     }
        # }

        # df = df_raw.copy()
        
        # day = df['day'].astype(int).values
        # open_p = df['open'].values
        # high_p = df['high'].values
        # low_p = df['low'].values
        # entry_signals = Conds.compute_any_conds(df, entry_condition)
        # exit_signals = Conds.compute_any_conds(df, exit_condition)
        
        # direction = 1
        # holding_periods = 15
        
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
        
        
        # winrate_arr_i = np.full(len(entry_signals), np.NaN)
        # avg_returns_arr_i = np.full(len(entry_signals), np.NaN)
        # profit_factor_arr_i = np.full(len(entry_signals), np.NaN)
        # return_arr_i = np.full(len(entry_signals), np.NaN)
        # upside_arr_i = np.full(len(entry_signals), np.NaN)
        # downside_arr_i = np.full(len(entry_signals), np.NaN)
        # max_runup_arr_i = np.full(len(entry_signals), np.NaN)
        # max_drawdown_arr_i = np.full(len(entry_signals), np.NaN)
        # avg_upside_arr_i = np.full(len(entry_signals), np.NaN)
        # avg_downside_arr_i = np.full(len(entry_signals), np.NaN)
        
    def run2(self, 
            trade_direction="Long", 
            use_shift = False,
            n_shift = 15,
            compute_start_time="2018_01_01", 
            use_holding_periods=True,
            holding_periods=60,
            use_takeprofit_cutloss=False,
            profit_thres=5,
            loss_thres=5
            ):
        
        df = self.df
        func = self.func
        params = self.params
        exit_params = self.exit_params
        name = self.name
        
        def test_params():
            trade_direction="Long"
            use_shift = False,
            n_shift = 15,
            use_holding_periods = True
            holding_periods=15
            profit_thres=5,
            loss_thres=5
            use_takeprofit_cutloss = False
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
                'use_cross': False,
                'direction': 'crossover',
                'cross_line': 'Upper band'
                }
            }
            
            exit_params = params.pop('exit_cond') if 'exit_cond' in params else {} 
            name = 'HPG'
        
# %%
        
        
        signals = func(df, params)
        
        df['signal'] = Utils.new_1val_series(True, df) if signals is None else signals
        df["signal"] = np.where((df["day"] < compute_start_time) | (df["signal"].isna()), False, df["signal"]).astype(bool)
        if use_shift:
            df["signal"] = df["signal"].shift(n_shift)
            df["signal"] = df['signal'].fillna(value=False)
                    

        exit_signals = Conds.compute_any_conds(df, exit_params)
        df["exitSignal"] = Utils.new_1val_series(False, df) if exit_signals is None else exit_signals
        
        df["name"] = name
        
        dfi = glob_obj.df_vnindex[['day', 'open', 'high', 'low', 'close']]
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

        
# %%
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
        self, trade_direction="Long", compute_start_time="2018_01_01", use_shift=False,
        n_shift=15, holding_periods=8
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

        
        signals = func(df, params)
        
        if signals is None:
            df["signal"] = Utils.new_1val_series(True, df) 
        else:
            df["signal"] = signals
        

        
            
        df["signal"] = np.where((df["day"] < compute_start_time) | (df["signal"].isna()), False, df["signal"]).astype(bool)
        if use_shift:
            df["signal"] = df["signal"].shift(n_shift)
            df["signal"] = df['signal'].fillna(value=False)
        # print(f"num of nan signals: {df['signal'].isna().sum()}")
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
        self, trade_direction="Long", compute_start_time="2018_01_01", use_shift=False,
        n_shift=15, holding_periods=8,
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

        
        signals = func(df, params)
        
        if signals is None:
            df["signal"] = Utils.new_1val_series(True, df) 
        else:
            df["signal"] = signals
        

        
            
        df["signal"] = np.where((df["day"] < compute_start_time) | (df["signal"].isna()), False, df["signal"]).astype(bool)
        if use_shift:
            df["signal"] = df["signal"].shift(n_shift)
            df["signal"] = df['signal'].fillna(value=False)
        # print(f"num of nan signals: {df['signal'].isna().sum()}")
        df["name"] = name

        # Compute trade stats using Numba
        # price_changes = df["priceChange"].values
        # returns = df["return"].values
        # returns1 = df["return1"].values
        # returns5 = df["return5"].values
        # returns10 = df["return10"].values
        # returns15 = df["return15"].values
        # signals = df["signal"].values
        # upsides = df["upside"].values
        # downsides = df["downside"].values
        # (
        #     trading_status,
        #     num_trades_arr,
        #     winrate_arr,
        #     winrate1_arr,
        #     winrate5_arr,
        #     winrate10_arr,
        #     winrate15_arr,
        #     avg_returns_arr,
        #     avg_returns1_arr,
        #     avg_returns5_arr,
        #     avg_returns10_arr,
        #     avg_returns15_arr,
        #     avg_upside_arr,
        #     avg_downside_arr,
        #     max_runup_arr,
        #     max_drawdown_arr,
        #     profit_factor_arr,
        #     is_entry_arr,
        # ) = self.compute_trade_stats3(
        #     price_changes, 
        #     returns, 
        #     returns1, 
        #     returns5, 
        #     returns10, 
        #     returns15, 
        #     signals, 
        #     upsides, 
        #     downsides, 
        #     holding_periods
        # )

        # Update DataFrame
        df['isEntry'] = df['signal'].copy()
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

        df['maxRunup'] = df['upside'].cummax()
        df['maxDrawdown'] = df['downside'].cummin()

        df['profit'] = df['isEntry'] * df['return'].clip(lower = 0)
        df['loss'] = df['isEntry'] * df['return'].clip(upper = 0) * (-1)

        df['profitFactor'] = np.where(df['loss']!=0, df['profit'].cumsum() / df['loss'].cumsum(), np.nan)
        
        df = df.drop(['is_win', 'is_win1', 'is_win5', 'is_win10', 'is_win15', 'profit', 'loss' ], axis = 1)
        df['isEntry'] = np.where(df['signal'], 1, np.nan)

        
        
        
        
        
        # df["numTrade"] = num_trades_arr
        # df["winrate"] = winrate_arr
        # df["winrateT1"] = winrate1_arr
        # df["winrateT5"] = winrate5_arr
        # df["winrateT10"] = winrate10_arr
        # df["winrateT15"] = winrate15_arr
        # df["avgReturn"] = avg_returns_arr
        # df["avgReturnT1"] = avg_returns1_arr
        # df["avgReturnT5"] = avg_returns5_arr
        # df["avgReturnT10"] = avg_returns10_arr
        # df["avgReturnT15"] = avg_returns15_arr
        # df["avgUpside"] = avg_upside_arr
        # df["avgDownside"] = avg_downside_arr
        # df["maxRunup"] = max_runup_arr
        # df["maxDrawdown"] = max_drawdown_arr
        # df["profitFactor"] = profit_factor_arr
        # df["isEntry"] = is_entry_arr
        # df["isTrading"] = trading_status

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
    

class Scanner:
    """Scanner"""

    @staticmethod
    def scan_multiple_stocks(
        func, 
        params, 
        stocks=None,
        trade_direction="Long", 
        use_shift=False,
        n_shift=15,
        holding_periods=60):
        """Backtest on multiple stocks"""
        from danglib.pylabview.celery_worker import scan_one_stock, clean_redis

        df_all_stocks = glob_obj.df_stocks
        if stocks is None:
            stocks = glob_obj.df_stocks
        else:
            df_all_stocks= df_all_stocks[df_all_stocks['stock'].isin(stocks)]

        clean_redis()
        
        sum_ls = []
        trades_ls = []
        err_stocks = {}
        task_dic = {}
        
        params = Globs.old_saved_adapters(params)
        
        for stock, df_stock in df_all_stocks.groupby("stock"):
            df_stock = df_stock.reset_index(drop=True)

            task = scan_one_stock.delay(
                df_stock, 
                func, 
                params=params,
                name=stock,
                trade_direction=trade_direction,
                use_shift=use_shift,
                n_shift=n_shift,
                holding_periods=holding_periods
            )
            
            task_dic[stock] = task
            
        while any(t.status!='SUCCESS' for t in task_dic.values()):
            pass

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
        ].round(2)
        res_df["beta_group"] = res_df["name"].map(glob_obj.dic_groups)

        trades_df = pd.concat(trades_ls)    
        trades_df['beta_group'] = trades_df['stock'].map(glob_obj.dic_groups)
        return res_df, trades_df
    
    @staticmethod
    def scan_multiple_stocks3(
        func, params, 
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
        # %%
        # func = Conds.compute_any_conds
        # stocks=None
        # trade_direction="Long"
        # holding_periods=15
        # params = {
        #     'china_hrc': {
        #         'ma_use_flag': True,
        #         'ma_len1': 5,
        #         'ma_len2': 15,
        #         'ma_type': 'EMA',
        #         'ma_dir': 'crossover',
        #         'hl_use_flag': False,
        #         'hl_dir': 'Increase',
        #         'hl_nbars': 10,
        #         'hl_lower': 5,
        #         'hl_upper': 100
        #     },
        #     'coking_coal': {
        #         'ma_use_flag': False,
        #         'ma_len1': 5,
        #         'ma_len2': 15,
        #         'ma_type': 'EMA',
        #         'ma_dir': 'crossover',
        #         'hl_use_flag': False,
        #         'hl_dir': 'Increase',
        #         'hl_nbars': 10,
        #         'hl_lower': 5,
        #         'hl_upper': 100
        #     },
        #     'iron_ore': {
        #         'ma_use_flag': False,
        #         'ma_len1': 5,
        #         'ma_len2': 15,
        #         'ma_type': 'EMA',
        #         'ma_dir': 'crossover',
        #         'hl_use_flag': False,
        #         'hl_dir': 'Increase',
        #         'hl_nbars': 10,
        #         'hl_lower': 5,
        #         'hl_upper': 100
        #     },
        #     # 'exit_cond':{
        #     #     'price_comp_ma':{
        #     #         "ma_len1":5,
        #     #         "ma_len2":15,
        #     #         "ma_type":"EMA",
        #     #         "ma_dir":"crossunder",
        #     #         "use_flag": True,
        #     #     }
        #     # }
        # }
        
        # exit_params = params.pop('exit_cond') if 'exit_cond' in params else {}
        """Backtest on multiple stocks"""  
        from danglib.pylabview.celery_worker import scan_one_stock_v2, clean_redis

        df_all_stocks = glob_obj.df_stocks
    
        if stocks is None:
            stocks = glob_obj.df_stocks
        else:
            df_all_stocks= df_all_stocks[df_all_stocks['stock'].isin(stocks)]

        clean_redis()
        
        sum_ls = []
        trades_ls = []
        err_stocks = {}
        task_dic = {}
        
        params = Globs.old_saved_adapters(params)
        
        for stock, df_stock in df_all_stocks.groupby("stock"):
            df_stock = df_stock.reset_index(drop=True)

            task = scan_one_stock_v2.delay(
                df_stock, 
                func, 
                params=params,
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
            
        while any(t.status!='SUCCESS' for t in task_dic.values()):
            pass

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
        ].round(2)
        res_df["beta_group"] = res_df["name"].map(glob_obj.dic_groups)

        trades_df = pd.concat(trades_ls)    
        trades_df['beta_group'] = trades_df['stock'].map(glob_obj.dic_groups)

# %%

        return res_df, trades_df

    @staticmethod
    def scan_multiple_stocks_v3(
        func, 
        params, 
        stocks=None,
        trade_direction="Long", 
        use_shift=False,
        n_shift=15,
        holding_periods=60):
        """Backtest on multiple stocks"""
        from danglib.pylabview.celery_worker import scan_one_stock_v3, clean_redis

        df_all_stocks = glob_obj.df_stocks
        if stocks is None:
            stocks = glob_obj.df_stocks
        else:
            df_all_stocks= df_all_stocks[df_all_stocks['stock'].isin(stocks)]

        clean_redis()
        
        sum_ls = []
        trades_ls = []
        err_stocks = {}
        task_dic = {}
        
        params = Globs.old_saved_adapters(params)
        
        for stock, df_stock in df_all_stocks.groupby("stock"):
            df_stock = df_stock.reset_index(drop=True)

            task = scan_one_stock_v3.delay(
                df_stock, 
                func, 
                params=params,
                name=stock,
                trade_direction=trade_direction,
                use_shift=use_shift,
                n_shift=n_shift,
                holding_periods=holding_periods
            )
            
            task_dic[stock] = task
            
        while any(t.status!='SUCCESS' for t in task_dic.values()):
            pass

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
        ].round(2)
        res_df["beta_group"] = res_df["name"].map(glob_obj.dic_groups)

        trades_df = pd.concat(trades_ls)    
        trades_df['beta_group'] = trades_df['stock'].map(glob_obj.dic_groups)
        return res_df, trades_df

glob_obj = Globs()
# glob_obj.load_stocks_data()
glob_obj.load_vnindex()
glob_obj.load_sectors_data()

def test():
    df = glob_obj.get_one_stock_data("HPG")

    params = (
        {
            'price_change':{
                'lower_thres':5,
                'use_flag': True
            }
        }
    )
    res, df = Scanner.scan_multiple_stocks3(
            func=Conds.compute_any_conds,
            params=params,
            stocks=['HPG', 'SSI', 'CTR'],
            trade_direction='Long', 
            use_holding_periods=True,
            holding_periods=15,
            use_takeprofit_cutloss=False,
            profit_thres=5,
            loss_thres=5
        )
    
    
if __name__ == "__main__":

    df_raw = glob_obj.get_one_stock_data("SSI")
    try:
        # Tạo một đối tượng ArgumentParser
        parser = argparse.ArgumentParser(description="A program to demonstrate command line arguments.")

        # Thêm các đối số
        parser.add_argument('--updatedata', action='store_true', help="Update stocks data for Pylabview")

        args = parser.parse_args()
        
        if args.updatedata:
            glob_obj.gen_stocks_data(send_viber=True)
    except:
        pass
    
# celery -A celery_worker worker --concurrency=10 --loglevel=INFO -n celery_worker@pylabview 