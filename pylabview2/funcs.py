import logging
from danglib.pylabview2.core_lib import pd, np, Ta, Adapters, Fns, Utils, Math
from danglib.pylabview.funcs import Conds as Series_Conds
from numba import njit
from pymongo import MongoClient
import logging
import warnings
from itertools import product
from danglib.chatbots.viberbot import F5bot
from danglib.utils import write_pickle, walk_through_files
from tqdm import tqdm
from dc_server.lazy_core import gen_plasma_functions

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
        self.function_map = {
            "price_change": Conds.price_change,
            "price_change_vs_hl": Conds.price_change_vs_hl,
            "price_comp_ma": Conds.price_comp_ma,
            "price_gap": Conds.price_gap,
            "price_highest_lowest": Conds.PriceAction.price_highest_lowest,
            "consecutive_conditional_bars": Conds.PriceAction.consecutive_conditional_bars,
            "vol_comp_ma": Conds.vol_comp_ma,
            "vol_percentile": Conds.vol_percentile,
            "consecutive_squeezes": Conds.consecutive_squeezes,
            "bbwp": Conds.bbwp,
            "bbpctb": Conds.bbpctb,
            "ursi": Conds.ursi,
            "macd": Conds.macd,
            "index_cond": Conds.index_cond,
            "lookback_cond": Conds.lookback_cond,

            "s_price_change": Series_Conds.price_change,
            "s_price_change_vs_hl": Series_Conds.price_change_vs_hl,
            "s_price_comp_ma": Series_Conds.price_comp_ma,
            "s_price_gap": Series_Conds.price_gap,
            "s_price_highest_lowest": Series_Conds.PriceAction.price_highest_lowest,
            "s_consecutive_conditional_bars": Series_Conds.PriceAction.consecutive_conditional_bars,
            "s_vol_comp_ma": Series_Conds.vol_comp_ma,
            "s_vol_percentile": Series_Conds.vol_percentile,
            "s_consecutive_squeezes": Series_Conds.consecutive_squeezes,
            "s_bbwp": Series_Conds.bbwp,
            "s_bbpctb": Series_Conds.bbpctb,
            "s_ursi": Series_Conds.ursi,
            "s_macd": Series_Conds.macd,

            "filter_sector": FilteringStocks.filter_sector,
            "filter_beta": FilteringStocks.filter_beta,
            "filter_marketcap_index": FilteringStocks.filter_marketcap_index,
            "filter_marketcap": FilteringStocks.filter_marketcap,
            "filter_ta_fa": FilteringStocks.filter_ta_fa
        }
        self.sectors = {}
        self.stocks_classified_df = None

    def get_sectors_stocks(self):

        def test():
            def_stocks = glob_obj.stocks
        def_stocks = self.stocks
        import ast
        
        db = MongoClient("localhost", 27022)["pylabview_db"]
        col = db['watchlist']
        df_raw = pd.DataFrame(col.find({},{'_id':0}))
        df_raw = df_raw[df_raw.index < 20]
        sectors = dict(zip(df_raw['watchlist_name'], df_raw['watchlist_params'].map(ast.literal_eval)))

        data_list = []
        for sector, stocks in sectors.items():
            for stock in stocks:
                if stock in def_stocks:
                    data_list.append({'stock': stock, 'sector': sector})

        df = pd.DataFrame(data_list)

        for stock in def_stocks:
            if stock not in df['stock'].tolist():
                data_list.append({'stock':stock, 'sector':'other'})

        df = pd.DataFrame(data_list)
        
        return df

    def load_stocks_data(self):
        """run to load stocks data"""
        df_stocks = Adapters.load_stocks_data_from_main_db()

        df_stocks = df_stocks.pivot(index='day', columns='stock')
        df_stocks['day'] = df_stocks.index

        self.df_stocks = df_stocks

    def load_stocks_data_pickle(self):
        """Load data from pickle"""
        df_stocks: pd.DataFrame = Adapters.load_stocks_data_from_pickle()

        df_stocks = df_stocks.pivot(index='day', columns='stock')
        df_stocks['day'] = df_stocks.index

        self.df_stocks = df_stocks

    def get_one_stock_data(self, stock):
        """Get one stock data from df_stocks"""
        df: pd.DataFrame = self.df_stocks[self.df_stocks["stock"] == stock].copy()
        df = df.reset_index(drop=True)
        return df
    
    def load_vnindex(self):
        self.df_vnindex: pd.DataFrame = Adapters.get_stock_from_vnstock(
            "VNINDEX", from_day=self.data_from_day
        )

    def gen_stocks_data(self, fn=Fns.pickle_stocks_data, collection=None, send_viber=False):
        try:
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

    def classify_stocks(self):
        df = self.get_sectors_stocks()
        dic_group = self.dic_groups
        stocks = self.stocks
        def test():
            df = glob_obj.get_sectors_stocks()
            dic_group = glob_obj.dic_groups
            stocks = glob_obj.stocks

        df['beta'] = df['stock'].map(dic_group)
        df['marketCapIndex'] = df['stock'].map(Adapters.classify_by_marketcap(stocks))
        self.stocks_classified_df = df


    def load_all_data(self):
        self.load_stocks_data()
        self.load_vnindex()
        self.load_sectors_data() 
        self.classify_stocks()


class Conds:
    """Custom condition funcitions"""

    class Standards:
        """Utility condition functions"""

        @staticmethod
        def two_line_pos(
            line1: pd.Series,
            line2: pd.Series,
            direction: str = "crossover",
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
                    res = line1 > line2
                elif direction == "below":
                    res = line1 < line2

            return res
        
        @staticmethod
        def comp_two_ma(
            df: pd.DataFrame,
            src_name:str,
            ma_len1=5,
            ma_len2=15,
            ma_type="EMA",
            ma_dir="above",
            use_flag: bool = True,
        ):
            if use_flag:
                src = df[src_name]
                ma1 = Ta.ma(src, ma_len1, ma_type)
                ma2 = Ta.ma(src, ma_len2, ma_type)

                return Conds.Standards.two_line_pos(
                    line1=ma1,
                    line2=ma2,
                    direction=ma_dir,
                    use_flag=True
                )
            
            return None
        
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
    def price_change(
        df: pd.DataFrame,
        src_name: str = "close",
        periods: int = 1,
        direction: str = "increase",
        lower_thres: float = 0,
        upper_thres: float = 10000,
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
        periods = int(periods)
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
        ma_len1 = int(ma_len1)
        ma_len2 = int(ma_len2)

        return Conds.Standards.comp_two_ma(
            df=df, 
            src_name='close', 
            ma_len1=ma_len1,
            ma_len2=ma_len2,
            ma_type=ma_type,
            ma_dir=ma_dir,
            use_flag=use_flag
        )
    
    @staticmethod
    def price_change_vs_hl(
        df: pd.DataFrame,
        src_name = 'close',
        use_flag: bool = False, 
        direction: str = "Increase",
        nbars: int = 10,
        low_range: float = 5,
        high_range: float = 10000
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
        nbars = int(nbars)
        close = df[src_name]
        comp_src = Ta.lowest(df['low'], nbars) if direction == 'Increase' else Ta.highest(df['high'], nbars)
        
        if use_flag:
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

            num_bars = int(num_bars)
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
                num_bars = int(num_bars)
                num_matched = int(num_matched)
                src1 = df[src1_name]
                src2 = df[src2_name]

                matched1 = Utils.count_changed(src1, num_bars, direction) >= num_matched
                matched2 = Utils.count_changed(src2, num_bars, direction) >= num_matched

                return matched1 & matched2

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
            n_bars=int(n_bars)
            ma_len=int(ma_len)
            v = df[v_type]

            comp_v = Ta.sma(v, n_bars)
            vol_ma = Ta.sma(v, ma_len)
            vcp = Utils.calc_percentage_change(vol_ma, comp_v)
            vcp *= 1 if comp_ma_dir == "higher" else -1
            avcp = Ta.sma(vcp, n_bars)

            return avcp >= comp_ma_perc

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
        ma_length = int(ma_length)
        v = df["volume"]
        if use_flag:
            ranking_window = int(ranking_window)
            ma_length = int(ma_length)

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
        bb_length = int(bb_length)
        length_kc = int(length_kc)
        num_bars = int(num_bars)

        if use_flag:
            num_bars = int(num_bars)
            # Calculating squeeze
            sqz_on, _, no_sqz  = Ta.squeeze(
                df, src_name, bb_length, length_kc, mult_kc, use_true_range
            )
            if use_no_sqz:
                return no_sqz
            else:
                # Count consecutive squeeze on
                cons_sqz_num = Utils.count_consecutive(sqz_on)
                return cons_sqz_num >= num_bars

        return None

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
        def test():
            df: pd.DataFrame = df_raw.copy()
            src_name: str = "close"
            basic_type: str = "SMA"
            bbwp_len: int = 13
            bbwp_lkbk: int = 128
            use_low_thres: bool = False
            low_thres: float = 20
            use_high_thres: bool = False
            high_thres: float = 80
            use_flag: bool = False
            
        bbwp_len = int(bbwp_len)
        bbwp_lkbk = int(bbwp_lkbk)

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
        direction: str = "crossover",
        cross_line: str = "Upper band",
        use_flag: bool = False,
    ):
        """bbpctb based conditions"""
        res = None

        length = int(length)

        if use_flag:
            bbpctb = Ta.bbpctb(df, src_name, length, mult)

            cross_l = Utils.new_1val_df(100 if cross_line == "Upper band" else 0, df[src_name])

            res = Conds.Standards.two_line_pos(
                line1=bbpctb,
                line2=cross_l,
                direction=direction,
                use_flag=use_flag,
            )

        return res
    
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
        def test():
            length: int = 14
            smo_type1: str = "RMA"
            smooth: int = 14
            smo_type2: str = "EMA"
            use_vs_signal: bool = True
            direction: str = "crossover"
            use_range: bool = True
            lower_thres: float = 0
            upper_thres: float = 0
            use_flag: bool = True

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
        def test():
            src_name: str = "close"
            r2_period: int = 20
            fast: int = 10
            slow: int = 20
            signal_length: int = 9
            use_vs_signal: bool = True
            direction: str = "crossover"
            use_range: bool = False
            lower_thres: float = 0
            upper_thres: float = 0
            use_flag: bool = True

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
    def index_cond(df: pd.DataFrame, function2, *args, **kwargs):
        """Compute index condition"""

        df2 = glob_obj.df_vnindex.copy()

        df2['cond']= Conds.compute_any_conds(df2, function2, *args, **kwargs)

        df2 = pd.merge(df['day'].reset_index(drop=True), df2, on='day', how='left' )
        df2['cond'] = df2['cond'].fillna(False)
        df2 = df2.set_index('day')  

        matched = df['close'].notna()
        matched = matched.apply(lambda col: col & df2['cond'])

        return matched
    
    @staticmethod
    def lookback_cond(df: pd.DataFrame, function2, *args, **kwargs):
        """Compute lookback condition"""
        lookback_cond: pd.DataFrame = Conds.compute_any_conds(df, function2, *args, **kwargs)
        return lookback_cond.rolling(5, closed = 'left').max().astype(bool)


    @staticmethod
    def compute_any_conds(df, function, *args, **kwargs):
        func = glob_obj.function_map[function]
        cond = func(df, *args, **kwargs)
        return cond

    @staticmethod
    def compute_any_conds2(df: pd.DataFrame, functions_params_dic: dict, *args):
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
    
class FilteringStocks:

    @staticmethod
    def filter_sector(
        values: list = None,
        use_flag: bool = False,
    ):
        if use_flag:
            df = glob_obj.stocks_classified_df
            df = df[df['sector'].isin(values)]
            return df['stock'].to_list()
        
        return None
    
    @staticmethod
    def filter_beta(
        values: list = None,
        use_flag: bool = False
    ):
        if use_flag:
            df = glob_obj.stocks_classified_df
            df = df[df['beta'].isin(values)]
            return df['stock'].to_list()
        
        return None
    
    @staticmethod
    def filter_marketcap_index(
        values: list = None,
        use_flag: bool = False
    ):
        if use_flag:
            df = glob_obj.stocks_classified_df
            df = df[df['marketCapIndex'].isin(values)]
            return df['stock'].to_list()
        
        return None
    
    @staticmethod
    def filter_marketcap(
        day = None,
        lower_range = 0,
        upper_range = 999999999,
        use_flag: bool = False
    ):
        def test():
            lower_range = 0
            upper_range = 3000
            day = '2024_08_08'

        if use_flag:
            marketcap: pd.Series = glob_obj.df_stocks.loc[day, 'marketCap']
            res = marketcap[(marketcap > lower_range )& (marketcap < upper_range)]
            return list(res.index)
        
        return None
    
    @staticmethod
    def filter_ta_fa(day, use_flag = False, **functions_params_dic):
        def test():
            functions_params_dic={
                'price_change': {
                    'use_flag': True,
                    'lower_thres': 2
                }
            }
        if use_flag:
            df = glob_obj.df_stocks

            matched: pd.Series = Conds.compute_any_conds2(df, functions_params_dic).loc[day]
            res = matched[matched == True]
            return list(res.index)
        
        return None
    
    @staticmethod
    def filter_all(filter_functions_params_dic: dict):
        def test():
            filter_functions_params_dic = {
                'filter_ta_fa':{
                    'day': '2024_08_08',
                    'use_flag':True,
                    'price_change': {
                        'use_flag': True,
                        'lower_thres': 2
                    },
                    'index_cond': {
                        'price_change':{
                            'use_flag': True,
                            'lower_thres': 3
                        }
                    }
                }
            }

        stocks_list = glob_obj.stocks
        try:
            for func_name, params in filter_functions_params_dic.items():
                if func_name == 'stock_scanner':
                    continue
                if(func_name not in glob_obj.function_map): 
                    continue
                func = glob_obj.function_map[func_name]
                tmp_ls = func(**params)
                if tmp_ls is not None:
                    stocks_list = list(set(stocks_list).intersection(tmp_ls))

        except Exception as e:
            logging.error(f"function `filter_all` error: {e}")

        return stocks_list


class Vectorized:
    @staticmethod
    def create_params_sets():

        params_dic = {
            'price1':{
                'function': 'price_change',
                'params': {
                    'periods': [1],
                    'lower_thres': [0, 3, 5, 6.7],
                    'direction': ['increase', 'decrease']
                 }
            },
            'price2':{
                'function':'price_change',
                'params' : {
                    'periods': [2],
                    'lower_thres': [6, 10, 13],
                    'direction': ['increase', 'decrease']
                }
            }, 
            'price3':{
                'function': 'price_change',
                'params' : {
                    'periods': [3],
                    'lower_thres': [9, 15, 19],
                    'direction': ['increase', 'decrease']
                }
            },
            'price4':{
                'function': 'price_change',
                'params' : {
                    'periods': [5],
                    'lower_thres': [10, 15, 20, 25],
                    'direction': ['increase', 'decrease']
                }
            },
            'price_ma1':{
                'function': 'price_comp_ma',
                'params': {
                    'ma_len1':[1],
                    'ma_len2':[5,10,15,20,50,200],
                    'ma_dir': ['above', 'below', 'crossover', 'crossunder'],
                }
            },
            'price_ma2':{
                'function': 'price_comp_ma',
                'params': {
                    'ma_len1':[5],
                    'ma_len2':[15,20],
                    'ma_dir': ['above', 'below', 'crossover', 'crossunder'],
                }
            },
            'price_ma3':{
                'function': 'price_comp_ma',
                'params': {
                    'ma_len1':[10],
                    'ma_len2':[30],
                    'ma_dir': ['above', 'below', 'crossover', 'crossunder'],
                }
            },
            'price_ma4':{
                'function': 'price_comp_ma',
                'params': {
                    'ma_len1':[20],
                    'ma_len2':[50, 100],
                    'ma_dir': ['above', 'below', 'crossover', 'crossunder'],
                }
            },
            'price_ma5':{
                'function': 'price_comp_ma',
                'params': {
                    'ma_len1':[50],
                    'ma_len2':[200],
                    'ma_dir': ['above', 'below', 'crossover', 'crossunder'],
                }
            },
            'price_hl1':{
                'function': 'price_change_vs_hl',
                'params': {
                    'direction': ['Increase','Decrease'],
                    'nbars': [5, 10, 15, 20, 30],
                    'low_range': [10, 15, 20, 25]
                }
            },
            'price_gap1':{
                'function': 'price_gap',
                'params':{
                    'gap_dir': ['Use Gap Up', 'Use Gap Down']
                }
            },
            'hlest1':{
                'function': 'price_highest_lowest',
                'params': {
                    'method': ['Highest', 'Lowest'],
                    'num_bars': [5, 10, 15, 20, 50, 200]
                }
            },
            'cons_bar1':{
                'function': 'consecutive_conditional_bars',
                'params': {
                    ('src1_name', 'src2_name'): [('close', 'close'), ('high', 'low'), ('low', 'low'), ('high', 'high'), ('high', 'close'), ('low', 'close')],
                    'direction': ['Increase', 'Decrease'],
                    ('num_bars', 'num_matched'): [(2,2), (3,3), (4,4), (5,5), (6,6), (7,7), (8,8)]
                }
            },
            'vol_comp_ma1':{
                'function': 'vol_comp_ma',
                'params' : {
                    ("n_bars", "ma_len") : [(1, 20), (1, 10), (1, 6), (2, 20), (3, 15)],
                    "comp_ma_dir": ["higher", "lower"],
                    "comp_ma_perc": [0, 25, 50, 75, 100, 150]
                }
            },
            # 'vol_perc1':{
            #     'function': 'vol_percentile',
            #     'params': {
            #         'ma_length': [1,3,5,10],
            #         'ranking_window': [64, 128, 256],
            #         'low_range': [2,5, 10, 15, 20],
            #         'high_range': [80, 90, 95, 98]
            #     }
            # },
            'vol_perc2':{
                'function': 'vol_percentile',
                'params': {
                    'ma_length': [1,3,5,10],
                    'ranking_window': [64, 128, 256],
                    'low_range': [0],
                    'high_range': [2,5, 10, 15, 20]
                }
            },
            'vol_perc3':{
                'function': 'vol_percentile',
                'params': {
                    'ma_length': [1,3,5,10],
                    'ranking_window': [64, 128, 256],
                    'low_range': [80, 90, 95, 98],
                    'high_range': [100]
                }
            },
            'squeeze1':{
                'function': 'consecutive_squeezes', 
                'params': {
                    'use_no_sqz': [True]
                }
            },
            'squeeze2':{
                'function': 'consecutive_squeezes', 
                'params': {
                    'num_bars': [3, 5, 10, 15, 20]
                }
            },
            'bbwp1' : {
                'function': 'bbwp',
                'params': {
                    ('use_low_thres', 'use_high_thres'):[(True, False), (False, True)],
                    'bbwp_len': [13, 30, 50],
                    'bbwp_lkbk': [64, 128, 256],
                    'low_thres': [2,5,10,20],
                    'high_thres': [80, 90, 95, 98]
                }
            },
            # 'bbwp2' : {
            #     'function': 'bbwp',
            #     'params': {
            #         ('use_low_thres', 'use_high_thres'):[(True, False), (False, True)],
            #         'bbwp_len': [13, 30, 50],
            #         'bbwp_lkbk': [64, 128, 256],
            #         'low_thres': [0],
            #         'high_thres': [2,5,10,20]
            #     }
            # },
            # 'bbwp3' : {
            #     'function': 'bbwp',
            #     'params': {
            #         ('use_low_thres', 'use_high_thres'):[(True, False), (False, True)],
            #         'bbwp_len': [13, 30, 50],
            #         'bbwp_lkbk': [64, 128, 256],
            #         'low_thres': [80, 90, 95, 98],
            #         'high_thres': [100]
            #     }
            # },
            'bbpctb1':{
                'function': 'bbpctb',
                'params':{
                'length': [20, 50, 100],
                'mult': [1.5, 2, 2.5, 3],
                'direction' : ['crossover', 'crossunder'],
                'cross_line': ['Upper band', 'Lower band']
                }
            },
            # 'ursi1': {
            #     'function': 'ursi',
            #     'params': {
            #         'direction': ['crossover', 'crossunder', 'above', 'below'],
            #         'use_vs_signal': [True],
            #         'use_range': [True],
            #         ('lower_thres', 'upper_thres'): [(0, 10), (0, 20), (10, 20), (20, 30), (80, 90), (90, 100), (80, 100)]
            #     }
            # },
            'ursi2': {
                'function': 'ursi',
                'params': {
                    'direction': ['crossover', 'crossunder', 'above', 'below'],
                    ('use_vs_signal','use_range'): [(True, False), (False,True), (True, True)],
                    ('lower_thres', 'upper_thres'): [(0, 10), (0, 20), (10, 20), (20, 30), (80, 90), (90, 100), (80, 100)]
                }
            },
            # 'macd1': {
            #     'function': 'macd',
            #     'params': {
            #         'use_vs_signal': [True],
            #         'use_range' : [True],
            #         'direction': ['crossover', 'crossunder'],
            #         ('lower_thres','upper_thres'):[(-9999,0), (0, 9999)]
            #     }
            # },
            'macd2': {
                'function': 'macd',
                'params': {
                    ('use_vs_signal','use_range'): [(True, False), (False,True), (True, True)],
                    'direction': ['crossover', 'crossunder'],
                    ('lower_thres','upper_thres'):[(-9999999,0), (0, 9999999)]
                }
            },
            'index1': {
                'function': 'index_cond',
                'params': {
                    'function2': ['s_price_comp_ma'],
                    ('ma_len1', 'ma_len2'): [(1,5), (1, 10), (1, 15), (1, 20), (1, 50), (1, 200), (5, 15), (5, 20), (10, 30), (20, 50), (20, 100), (50, 200)],
                    'ma_dir': ['above', 'below', 'crossover', 'crossunder']
                }
            },
            'index2': {
                'function': 'index_cond',
                'params': {
                    'function2': ['s_bbwp'],
                    ('use_low_thres', 'use_high_thres'):[(True, False), (False, True)],
                    'bbwp_len': [13, 30, 50],
                    'bbwp_lkbk': [64, 128, 256],
                    'low_thres': [2,5,10,20],
                    'high_thres': [80, 90, 95, 98]
                }
            },
            'index3': {
                'function': 'index_cond', 
                'params': {
                    'function2': ['s_consecutive_squeezes'],
                    'use_no_sqz': [True]
                }
            },
            'index4': {
                'function': 'index_cond', 
                'params': {
                    'function2': ['s_consecutive_squeezes'],
                    'num_bars': [3, 5, 10, 15, 20]
                }
            },
            # 'index5': {
            #     'function': 'index_cond', 
            #     'params': {
            #         'function2': ['s_ursi'], 
            #         'direction': ['crossover', 'crossunder', 'above', 'below'],
            #         'use_vs_signal': [True],
            #         'use_range': [True],
            #         ('lower_thres', 'upper_thres'): [(0, 10), (0, 20), (10, 20), (20, 30), (80, 90), (90, 100), (80, 100)]
            #     }
            # },
            'index6': {
                'function': 'index_cond', 
                'params': {
                    'function2': ['s_ursi'], 
                    'direction': ['crossover', 'crossunder', 'above', 'below'],
                    ('use_vs_signal','use_range'): [(True, False), (False,True), (True, True)],
                    ('lower_thres', 'upper_thres'): [(0, 10), (0, 20), (10, 20), (20, 30), (80, 90), (90, 100), (80, 100)]
                }
            },
            'lkbk1':{
                'function': 'lookback_cond',
                'params': {
                    'function2': ['price_change'],
                    'periods': [1],
                    'lower_thres': [0, 3, 5, 6.7],
                    'direction': ['increase', 'decrease']
                 }
            },
            'lkbk2':{
                'function': 'lookback_cond',
                
                'params' : {
                    'function2':['price_change'],
                    'periods': [2],
                    'lower_thres': [6, 10, 13],
                    'direction': ['increase', 'decrease']
                }
            }, 
            'lkbk3':{
                'function': 'lookback_cond',
                
                'params' : {
                    'function2': ['price_change'],
                    'periods': [3],
                    'lower_thres': [9, 15, 19],
                    'direction': ['increase', 'decrease']
                }
            },
            'lkbk4':{
                'function': 'lookback_cond',
                
                'params' : {
                    'function2': ['price_change'],
                    'periods': [5],
                    'lower_thres': [10, 15, 20, 25],
                    'direction': ['increase', 'decrease']
                }
            },
            'lkbk5':{
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['price_comp_ma'],
                    'ma_len1':[1],
                    'ma_len2':[5,10,15,20,50,200],
                    'ma_dir': ['above', 'below', 'crossover', 'crossunder'],
                }
            },
            'lkbk6':{
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['price_comp_ma'],
                    'ma_len1':[5],
                    'ma_len2':[15,20],
                    'ma_dir': ['above', 'below', 'crossover', 'crossunder'],
                }
            },
            'lkbk7':{
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['price_comp_ma'],
                    'ma_len1':[10],
                    'ma_len2':[30],
                    'ma_dir': ['above', 'below', 'crossover', 'crossunder'],
                }
            },
            'lkbk8':{
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['price_comp_ma'],
                    'ma_len1':[20],
                    'ma_len2':[50, 100],
                    'ma_dir': ['above', 'below', 'crossover', 'crossunder'],
                }
            },
            'lkbk9':{
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['price_comp_ma'],
                    'ma_len1':[50],
                    'ma_len2':[200],
                    'ma_dir': ['above', 'below', 'crossover', 'crossunder'],
                }
            },
            'lkbk10':{
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['price_change_vs_hl'],
                    'direction': ['Increase','Decrease'],
                    'nbars': [5, 10, 15, 20, 30],
                    'low_range': [10, 15, 20, 25]
                }
            },
            'lkbk11':{
                'function': 'lookback_cond',
                
                'params':{
                    'function2': ['price_gap'],
                    'gap_dir': ['Use Gap Up', 'Use Gap Down']
                }
            },
            'lkbk12':{
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['price_highest_lowest'],
                    'method': ['Highest', 'Lowest'],
                    'num_bars': [5, 10, 15, 20, 50, 200]
                }
            },
            'lkbk13':{
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['consecutive_conditional_bars'],
                    ('src1_name', 'src2_name'): [('close', 'close'), ('high', 'low'), ('low', 'low'), ('high', 'high'), ('high', 'close'), ('low', 'close')],
                    'direction': ['Increase', 'Decrease'],
                    ('num_bars', 'num_matched'): [(2,2), (3,3), (4,4), (5,5), (6,6), (7,7), (8,8)]
                }
            },
            'lkbk14':{
                'function': 'lookback_cond',
                
                'params' : {
                    'function2': ['vol_comp_ma'],
                    ("n_bars", "ma_len") : [(1, 20), (1, 10), (1, 6), (2, 20), (3, 15)],
                    "comp_ma_dir": ["higher", "lower"],
                    "comp_ma_perc": [0, 25, 50, 75, 100, 150]
                }
            },
            # 'lkbk15':{
            #     'function': 'lookback_cond',
                
            #     'params': {
            #         'function2': ['vol_percentile'],
            #         'ma_length': [1,3,5,10],
            #         'ranking_window': [64, 128, 256],
            #         'low_range': [2,5, 10, 15, 20],
            #         'high_range': [80, 90, 95, 98]
            #     }
            # },
            'lkbk151':{
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['vol_percentile'],
                    'ma_length': [1,3,5,10],
                    'ranking_window': [64, 128, 256],
                    'low_range': [0],
                    'high_range': [2,5, 10, 15, 20]
                }
            },
            'lkbk152':{
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['vol_percentile'],
                    'ma_length': [1,3,5,10],
                    'ranking_window': [64, 128, 256],
                    'low_range': [80, 90, 95, 98],
                    'high_range': [100]
                }
            },
            'lkbk16':{
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['consecutive_squeezes'], 
                    'use_no_sqz': [True]
                }
            },
            'lkbk17':{
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['consecutive_squeezes'], 
                    'num_bars': [3, 5, 10, 15, 20]
                }
            },
            'lkbk18' : {
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['bbwp'],
                    ('use_low_thres', 'use_high_thres'):[(True, False), (False, True)],
                    'bbwp_len': [13, 30, 50],
                    'bbwp_lkbk': [64, 128, 256],
                    'low_thres': [2,5,10,20],
                    'high_thres': [80, 90, 95, 98]
                }
            },
            'lkbk19':{
                'function': 'lookback_cond',
                
                'params':{
                    'function2': ['bbpctb'],
                    'length': [20, 50, 100],
                    'mult': [1.5, 2, 2.5, 3],
                    'direction' : ['crossover', 'crossunder'],
                    'cross_line': ['Upper band', 'Lower band']
                }
            },
            # 'lkbk20': {
            #     'function': 'lookback_cond',
                
            #     'params': {
            #         'function2': ['ursi'],
            #         'direction': ['crossover', 'crossunder', 'above', 'below'],
            #         'use_vs_signal': [True],
            #         'use_range': [True],
            #         ('lower_thres', 'upper_thres'): [(0, 10), (0, 20), (10, 20), (20, 30), (80, 90), (90, 100), (80, 100)]
            #     }
            # },
            'lkbk201': {
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['ursi'],
                    'direction': ['crossover', 'crossunder', 'above', 'below'],
                    ('use_vs_signal','use_range'): [(True, False), (False,True), (True, True)],
                    ('lower_thres', 'upper_thres'): [(0, 10), (0, 20), (10, 20), (20, 30), (80, 90), (90, 100), (80, 100)]
                }
            },
            # 'lkbk21': {
            #     'function': 'lookback_cond',
                
            #     'params': {
            #         'function2': ['macd'],
            #         'use_vs_signal': [True],
            #         'use_range' : [True],
            #         'direction': ['crossover', 'crossunder'],
            #         ('lower_thres','upper_thres'):[(-9999,0), (0, 9999)]
            #     }
            # },
            'lkbk211': {
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['macd'],
                    ('use_vs_signal','use_range'): [(True, False), (False,True), (True, True)],
                    'direction': ['crossover', 'crossunder'],
                    ('lower_thres','upper_thres'):[(-9999,0), (0, 9999)]
                }
            },
        }
        
        params_df: pd.DataFrame = None
        for cluster, val in params_dic.items():
            f = val['function']
            p: dict = val['params']

            combinations = list(product(*p.values()))
            dff = pd.DataFrame(combinations, columns=p.keys())
            for c in dff.columns:
                if not isinstance(c, str) :
                    dff[list(c)] = pd.DataFrame(dff[c].tolist(), index=dff.index)
                    dff = dff.drop(c, axis=1)

            dff['function'] = f

            params_df = pd.concat([params_df, dff], ignore_index=True)

        t = params_df[params_df['function'] == 'index_cond'].dropna(axis=1, how='all')
        return params_df
    
    @staticmethod
    def compute_conditions(df, to_pickle= False):
        from tqdm import tqdm
        def test():
            df = df_raw.copy()

        params_df:pd.DataFrame = Vectorized.create_params_sets()
        # params_df = params_df[params_df['function'] == 'index_cond']

        sig_dic = {}
        for idx, params in tqdm(params_df.iterrows(), total=params_df.shape[0]):

            try:
                params = params.dropna().to_dict()
                for k, v in params.items():
                    for patt in ['len', 'bar', 'lkbk', 'mult', 'period', 'ranking']:
                        k: str
                        if patt in k:
                            params[k] = int(v)
                params['use_flag'] = True

                signals: pd.DataFrame = Conds.compute_any_conds(df, **params)

                sig_dic[idx] = signals

            except Exception as e:
                print(idx)
                print(e)

        if to_pickle:
            params_df.to_pickle("/home/ubuntu/Dang/pickles/params_df.pkl")
            write_pickle(sig_dic, "/home/ubuntu/Dang/pickles/signal_dict.pkl")

        return params_df, sig_dic
    
    @staticmethod
    def compute_all(df: pd.DataFrame, recompute_signals = True):
        from danglib.pylabview2.celery_worker import compute_signal, clean_redis, compute_multi_strategies
        _, client_disconnect, psave, pload = gen_plasma_functions()

        psave("df_stocks", df)

        df_return: pd.DataFrame  = df['open'].pct_change(15).shift(-16) * 100
        df_return = get_from_day(df_return, '2018_01_01')
        return_num = df_return.to_numpy()

        stocks_map = {k: v for k, v in enumerate(df_return.columns)}

        psave("return_array", return_num)

        def compute_signals():
            params_df: pd.DataFrame = Vectorized.create_params_sets()
            n_strats = len(params_df)

            if recompute_signals:
                df_tmp = df['close']
                df_tmp = df_tmp[df_tmp.index >= '2018_01_01']
                n_rows, n_cols = df_tmp.shape

                # Khởi tạo một mảng NumPy 3 chiều với kích thước (2000, 2000, 200)
                array_3d = np.empty((n_strats, n_rows, n_cols))

                task_dic = {}
                print("Computing signals:")
                for idx, params in tqdm(params_df.iterrows(), total=params_df.shape[0]):    
                    params = params.dropna().to_dict()
                    task_dic[idx] = compute_signal.delay(idx, params)

                while any(t.status!='SUCCESS' for t in task_dic.values()):
                    pass
                
                for idx, v in task_dic.items():
                    res: pd.DataFrame = v.result
                    if res is not None:
                        array_3d[idx, :, :] = res

                    else:
                        print(f"{idx} error")
                array_3d = array_3d.astype(bool)
                psave("sig_3d_array",array_3d)
                clean_redis()

            return n_strats

        def compute_stats(n):
            clean_redis()

            task_dic = {}
            print("Computing stats")
            for i in tqdm(range(0, n-1)):
                task_dic[i] = compute_multi_strategies.delay(i)

            while any(t.status!='SUCCESS' for t in task_dic.values()):
                pass

            clean_redis()

        def join_data(n):
            fns = walk_through_files("/data/dang/tmp/")

            def join_one_stats(name):

                # df_res: pd.DataFrame = None
                res = []
                print(f"Join {name} stats: ")
                for f in tqdm(fns):
                    f: str
                    i = int(f.split('/')[-1].split(".")[0].split("_")[1])
                    if i < 2013:
                        nt_raw, re_raw, wt_raw = pd.read_pickle(f)
                        src = None
                        if name == 'nt':
                            src = nt_raw
                        if name == 're':
                            src = re_raw
                        if name == 'wt':
                            src = wt_raw

                        tmp = pd.DataFrame(src, index=list(range(i+1, n)))
                        tmp[-1] = i 
                        res.append(tmp)
                res: pd.DataFrame = pd.concat(res)
                res = res.reset_index(names=-2)
                cols_map = stocks_map.copy()
                cols_map[-1] = 'i'
                cols_map[-2] = 'j'

                res = res.rename(columns = cols_map)
                res = res.set_index(['i', 'j'])
                write_pickle(f"/home/ubuntu/Dang/pickles/df_{name}.pkl", res)

            join_one_stats('nt')
            join_one_stats('wt')
            join_one_stats('re')



        n = compute_signals()
        compute_stats(n)
        join_data(n)

        client_disconnect()

def run():     
    Vectorized.compute_all(df, recompute_signals=True)
        

glob_obj = Globs()


def get_from_day(df: pd.DataFrame, from_day, col = None):
    if col is not None:
        df = df[df[col] >= from_day]    
    else:
        df = df[df.index >= from_day]
    return df

if __name__ == "__main__":
    glob_obj.load_all_data()
    df_raw = glob_obj.df_stocks.copy()
    df = df_raw.copy()
