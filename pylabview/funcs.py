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
            "hpg_margin": Conds.Sectors.hpg_margin
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
                line1, line2, direction, use_vs_signal
            )
            range_cond = Conds.Standards.range_cond(
                line1, lower_thres, upper_thres, use_range
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
        
        close = df[src_name]
        comp_src = Ta.lowest(df['low'], nbars) if direction == 'Increase' else Ta.highest(df['high'], nbars)
        
        if use_flag:
            pct_change = Utils.calc_percentage_change(comp_src, close)
            if direction == "Decrease":
                pct_change = pct_change * -1

            return Utils.in_range(pct_change, low_range, high_range, equal=False)

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

            cross_l = Utils.new_1val_series(1 if cross_line == "Upper band" else 0, df)

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
        def net_income(
            df: pd.DataFrame,
            calc_type: str = "QoQ",
            roll_len: int = 1,
            direction: str = "positive",
            percentage: float = 0,
            use_shift: bool = False,
            n_shift: int = 5,
            use_flag: bool = False,
        ):
            """Net Income based conditions"""

            if use_flag:
                df_ni = df.groupby("mapYQ")[["netIncome"]].max()
                df_ni["rollSum"] = Math.sum(df_ni["netIncome"], roll_len)

                pct_change_periods = 1 if calc_type == "QoQ" else 4

                df_ni["pctChange"] = (
                    Math.pct_change(df_ni["rollSum"], pct_change_periods) * 100
                ).round(6)

                df_ni["matched"] = (
                    df_ni["pctChange"] > percentage
                    if direction == "positive"
                    else df_ni["pctChange"] < -percentage
                )
                
                df_ni["matched"] = np.where(
                    (np.isinf(df_ni["pctChange"])) | (df_ni['rollSum'] == 0),
                    False,
                    df_ni["matched"]
                )

                matched = df["mapYQ"].map(df_ni["matched"])

                if use_shift:
                    matched = matched.shift(n_shift)
                    matched = pd.Series(
                        data=np.where(matched.isna(), False, matched), 
                        index=matched.index
                    )
                    
                # df['matched'] = matched

                return matched

            return None
        
        @staticmethod
        def revenue(
            df: pd.DataFrame,
            calc_type: str = "QoQ",
            roll_len: int = 1,
            direction: str = "positive",
            percentage: float = 0,
            use_shift: bool = False,
            n_shift: int = 5,
            use_flag: bool = False,
        ):
            """Net Income based conditions"""

            if use_flag:
                df_ni = df.groupby("mapYQ")[["revenue"]].max()
                df_ni["rollSum"] = Math.sum(df_ni["revenue"], roll_len)

                pct_change_periods = 1 if calc_type == "QoQ" else 4

                df_ni["pctChange"] = (
                    Math.pct_change(df_ni["rollSum"], pct_change_periods) * 100
                ).round(6)

                df_ni["matched"] = (
                    df_ni["pctChange"] > percentage
                    if direction == "positive"
                    else df_ni["pctChange"] < -percentage
                )
                
                df_ni["matched"] = np.where(
                    (np.isinf(df_ni["pctChange"])) | (df_ni['rollSum'] == 0),
                    False,
                    df_ni["matched"]
                )

                matched = df["mapYQ"].map(df_ni["matched"])

                if use_shift:
                    matched = matched.shift(n_shift)
                    matched = pd.Series(
                        data=np.where(matched.isna(), False, matched), 
                        index=matched.index
                    )
                    
                # df['matched'] = matched

                return matched

            return None
# PE_useRange = input.bool(false, title = 'Range Cond', inline = "PErange", group = 'P/E conditions')
# PE_highrange = input.float(10, minval =0,title = 'P/E High Range',inline = "PErange", group = 'P/E conditions')
# PE_lowrange = input.float(0, minval =0 , title = 'P/E Low Range' ,inline = "PErange", group = 'P/E conditions')
# PE_plot = input.bool(false, title = 'plot P/E')

# PB_useRange = input.bool(false, title = 'Range Cond', inline = "PBrange", group = 'P/B conditions')
# PB_highrange = input.float(10, minval =0,title = 'P/B High Range',inline = "PBrange", group = 'P/B conditions')
# PB_lowrange = input.float(0, minval =0 , title = 'P/B Low Range' ,inline = "PBrange", group = 'P/B conditions')

# PB_BB_length = input.int(20, minval=1,title ='P/B MA Length', group = 'P/B conditions')
# PB_BB_mult = input.float(2.0, minval=0.001, maxval=50, title="P/B StdDev",group = 'P/B conditions')
# PB_useUpperMethod = input.bool(defval = false, title = '', inline = 'BBUpdir', group = 'P/B conditions')
# PB_upperMethod = input.string('above', options = ['above', 'below', 'crossover', 'crossunder'], title = 'P/B vs UpperBand',inline = 'BBUpdir', group = 'P/B conditions')
# PB_useLowerMethod = input.bool(defval = false, title = '', inline = 'BBLowdir',group = 'P/B conditions') 
# PB_lowerMethod = input.string('below', options = ['above', 'below', 'crossover', 'crossunder'], title = 'P/B vs LowerBand', inline = 'BBLowdir',group = 'P/B conditions')
# PB_plot = input.bool(false, title = 'plot  P/B', group = 'P/B conditions')

# f_PECond(PriceEarningsRatio) =>
#     EPSCond = not PE_useRange or dlib.f_inrange(PriceEarningsRatio, PE_lowrange, PE_highrange)
#     EPSCond

# f_PBCond(PriceBookRatio)=>
#     PBRangeCond = (not PB_useRange or dlib.f_inrange(PriceBookRatio, PB_lowrange, PB_highrange))

#     PB_basis = ta.sma(PriceBookRatio, PB_BB_length)
#     PB_dev = PB_BB_mult * ta.stdev(PriceBookRatio, PB_BB_length)
#     PB_upper = PB_basis + PB_dev
#     PB_lower = PB_basis - PB_dev

#     PB_upperCond = not PB_useUpperMethod or f_define_2line_cond(PriceBookRatio, PB_upper, PB_upperMethod, true)
#     PB_lowerCond = not PB_useLowerMethod or f_define_2line_cond(PriceBookRatio, PB_lower, PB_lowerMethod, true)

#     PBBBCond = PB_upperCond and PB_lowerCond // Điều kiện upper lower bollinger Band
#     PBCond = PBBBCond and PBRangeCond

# f_PEPBCond(PriceEarningsRatio, PriceBookRatio) =>
#     f_PECond(PriceEarningsRatio) and f_PBCond(PriceBookRatio)

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

            
    class Sectors:
        @staticmethod
        def steel_ma_and_hl(
            df: pd.DataFrame, 
            steel_src,
            ma_use_flag: bool = True, # Coking Coal MA combination
            ma_type = 'EMA', # Method
            ma_len1 = 5, # MA1
            ma_len2 = 15, # MA2
            ma_dir = 'crossover', # Direction
            hl_use_flag: bool = False, # Increase/Decrease over N bars
            hl_nbars = 5, # N Bars
            hl_dir = 'increase', # Direction
            hl_lower = -999, # Lower
            hl_upper = 999 # Upper
        ):
            
            # steel_src='Aus Coal'
            # ma_use_flag: bool = False # Coking Coal MA combination
            # ma_type = 'EMA' # Method
            # ma_len1 = 5 # MA1
            # ma_len2 = 15 # MA2
            # ma_dir = 'crossover' # Direction
            # hl_use_flag: bool = False # Increase/Decrease over N bars
            # hl_nbars = 5 # N Bars
            # hl_dir = 'increase' # Direction
            # hl_lower = -999 # Lower
            # hl_upper = 999 # Upper
            # df = glob_obj.get_one_stock_data("HPG")
        
            df_steel = glob_obj.sectors['steel']['io_data'].copy()
            df_steel['high'] = df_steel['low'] = df_steel[steel_src].copy()
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
            ma_use_flag: bool = True, # Coking Coal MA combination
            ma_type = 'EMA', # Method
            ma_len1 = 5, # MA1
            ma_len2 = 15, # MA2
            ma_dir = 'crossover', # Direction
            hl_use_flag: bool = False, # Increase/Decrease over N bars
            hl_nbars = 5, # N Bars
            hl_dir = 'increase', # Direction
            hl_lower = -999, # Lower
            hl_upper = 999 # Upper
        ):
            return Conds.Sectors.steel_ma_and_hl(
                df = df,
                steel_src='Aus Coal',
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
            ma_use_flag: bool = True, # Coking Coal MA combination
            ma_type = 'EMA', # Method
            ma_len1 = 5, # MA1
            ma_len2 = 15, # MA2
            ma_dir = 'crossover', # Direction
            hl_use_flag: bool = False, # Increase/Decrease over N bars
            hl_nbars = 5, # N Bars
            hl_dir = 'increase', # Direction
            hl_lower = -999, # Lower
            hl_upper = 999 # Upper
        ):
            return Conds.Sectors.steel_ma_and_hl(
                df = df,
                steel_src='Ore 62',
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
            ma_use_flag: bool = True, # Coking Coal MA combination
            ma_type = 'EMA', # Method
            ma_len1 = 5, # MA1
            ma_len2 = 15, # MA2
            ma_dir = 'crossover', # Direction
            hl_use_flag: bool = False, # Increase/Decrease over N bars
            hl_nbars = 5, # N Bars
            hl_dir = 'increase', # Direction
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
            ma_use_flag: bool = True, # Coking Coal MA combination
            ma_type = 'EMA', # Method
            ma_len1 = 5, # MA1
            ma_len2 = 15, # MA2
            ma_dir = 'crossover', # Direction
            hl_use_flag: bool = False, # Increase/Decrease over N bars
            hl_nbars = 5, # N Bars
            hl_dir = 'increase', # Direction
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
            ma_use_flag: bool = True, # Coking Coal MA combination
            ma_type = 'EMA', # Method
            ma_len1 = 5, # MA1
            ma_len2 = 15, # MA2
            ma_dir = 'crossover', # Direction
            hl_use_flag: bool = False, # Increase/Decrease over N bars
            hl_nbars = 5, # N Bars
            hl_dir = 'increase', # Direction
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
            ma_use_flag: bool = True, # Coking Coal MA combination
            ma_type = 'EMA', # Method
            ma_len1 = 5, # MA1
            ma_len2 = 15, # MA2
            ma_dir = 'crossover', # Direction
            hl_use_flag: bool = False, # Increase/Decrease over N bars
            hl_nbars = 5, # N Bars
            hl_dir = 'increase', # Direction
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

    @staticmethod
    def compute_any_conds(df: pd.DataFrame, functions_params_dic: dict):
        """Compute and combine any conditions"""
        df = df.copy()
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
        
        total_return = 0.0
        total_return1 = 0.0
        total_return5 = 0.0
        total_return10 = 0.0
        total_return15 = 0.0
        
        total_upside = 0
        total_downside = 0
        
        
        total_profit = 0.0
        total_loss = 0.0
        
        
        max_runup = np.NaN
        max_drawdown = np.NaN

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
        
        for bar_index, signal in enumerate(entry_signals):
            if signal and not is_trading:
                is_trading = True
                start_bar = bar_index
                is_entry_arr[bar_index] = 1
                
                num_trades = num_trades + 1
                num_trades_arr[start_bar] = num_trades

                trade_upside = np.NaN
                trade_downside = np.NaN
                
                entry_price = src_idx(open_p, bar_index+1)
                entry_price_arr[start_bar] = entry_price
                entry_day_arr[start_bar] = day[bar_index]
                
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
                trading_status[bar_index] = 1
                
            # match_exit = (
            #     (bar_index - start_bar >= 1) & (exit_signals[bar_index])
            # ) | (
            #     (bar_index - start_bar) == holding_periods
            # )
            
            exit_by_signals = (bar_index - start_bar >= 1) & (exit_signals[bar_index])
            exit_by_periods = use_holding_periods and ((bar_index - start_bar) == holding_periods)
            
            exit_by_periods = False
            if (bar_index - start_bar >= 1):
                pc = Utils.calc_percentage_change(entry_price, close_p[bar_index])
                exit_by_periods = use_takeprofit_cutloss and ((pc >= profit_thres) or (pc <= -loss_thres))

            match_exit = exit_by_signals or exit_by_periods or exit_by_periods
            
            if is_trading and match_exit:
                
                is_trading = False
                
                close_price = src_idx(open_p, bar_index+1)

                price_change = (close_price - entry_price) * direction
                
                trade_return = price_change / entry_price * 100
                
                if trade_return > 0:
                    num_win = num_win + 1
                    total_profit = total_profit + price_change
 
                if trade_return < 0:
                    total_loss = total_loss + price_change
                
                total_return = total_return + trade_return
                
                trade_upside =  max(trade_upside, (close_price - entry_price) / entry_price * 100)
                trade_downside =  min(trade_downside,  (close_price - entry_price) / entry_price * 100)
            
                max_runup = trade_upside if np.isnan(max_runup) else max(max_runup, trade_upside)
                max_drawdown = trade_downside if np.isnan(max_drawdown) else min(max_drawdown, trade_downside)

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
                exit_day_arr[start_bar] = day[bar_index]
                return_arr[start_bar] = trade_return
                price_change_arr[start_bar] = price_change
                
                
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
        )
        
    def run2(self, 
            trade_direction="Long", 
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
        
# %%
        # trade_direction="Long"
        # holding_periods=15
        # compute_start_time="2018_01_01"
        # df = df_raw.copy()
        # func = Conds.compute_any_conds
        
        # params = {
        # 'bbpctb': {
        #     'src_name': 'close',
        #     'length': 20,
        #     'mult': 2,
        #     'use_flag': True,
        #     'use_range': False,
        #     'low_range': 80,
        #     'high_range': 100,
        #     'use_cross': False,
        #     'direction': 'crossover',
        #     'cross_line': 'Upper band'
        #     }
        # }
        
        # exit_params = params.pop('exit_cond') if 'exit_cond' in params else {} 
        # name = 'HPG'
        
        
        signals = func(df, params)
        
        df['signal'] = Utils.new_1val_series(True, df) if signals is None else signals
        df["signal"] = np.where((df["day"] < compute_start_time) | (df["signal"].isna()), False, df["signal"]).astype(bool)

        exit_signals = Conds.compute_any_conds(df, exit_params)
        df["exitSignal"] = Utils.new_1val_series(False, df) if exit_signals is None else exit_signals
        
        df["name"] = name
        
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
            price_change_arr
        )=Simulator.compute_trade_stats2(
            day=df['day'].astype(int).values,
            open_p=df['open'].values, 
            high_p=df['high'].values,
            low_p=df['low'].values,
            close_p=df['close'].values,
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

        
# %%
        self.df = df    
        self.result = result
# %%
        
    

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
        self, trade_direction="Long", compute_start_time="2018_01_01", holding_periods=8
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

class Scanner:
    """Scanner"""

    @staticmethod
    def scan_multiple_stocks(func, params, stocks=None, trade_direction="Long", holding_periods=60):
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
        
        for stock, df_stock in df_all_stocks.groupby("stock"):
            df_stock = df_stock.reset_index(drop=True)

            task = scan_one_stock.delay(
                df_stock, 
                func, 
                params=params,
                name=stock,
                trade_direction=trade_direction,
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
    def scan_multiple_stocks2(func, params, trade_direction="Long", holding_periods=60):
        df_all_stocks = glob_obj.df_stocks

        res_ls = []
        df_ls = []
        err_stocks = {}

        for stock, df_stock in df_all_stocks.groupby("stock"):
            df_stock = df_stock.reset_index(drop=True)
            try:
                bt = Simulator(
                    func,
                    df_ohlcv=df_stock,
                    params=params,
                    name=stock,
                )
                bt.run(trade_direction, holding_periods=holding_periods)
                res = bt.result
                df_trades = bt.df
                df_trades["beta_group"] = glob_obj.dic_groups[stock]

                if res is not None:
                    res_ls.append(res)
                    df_ls.append(df_trades)
                else:
                    err_stocks[stock] = "Empty simmulated result"
            except ValueError as e:
                err_stocks[stock] = e

        if Globs.verbosity == 1:
            logging.info("Stocks' scanned errors:")
            pprint.pprint(err_stocks)

        res_df = pd.DataFrame(res_ls)
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

        trades_df = pd.concat(df_ls)

        return res_df, trades_df
    
    @staticmethod
    def scan_multiple_stocks3(
        func, params, 
        stocks=None, 
        trade_direction="Long", 
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
        
        for stock, df_stock in df_all_stocks.groupby("stock"):
            df_stock = df_stock.reset_index(drop=True)

            task = scan_one_stock_v2.delay(
                df_stock, 
                func, 
                params=params,
                name=stock,
                trade_direction=trade_direction,
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
            ]
        ].round(2)
        res_df["beta_group"] = res_df["name"].map(glob_obj.dic_groups)

        trades_df = pd.concat(trades_ls)    
        trades_df['beta_group'] = trades_df['stock'].map(glob_obj.dic_groups)

# %%

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

    df_raw = glob_obj.get_one_stock_data("HPG")
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