import logging
from danglib.pylabview2.core_lib import pd, np, Ta, Adapters, Fns, Utils, Math
from danglib.pylabview.funcs import Conds as Series_Conds, glob_obj as glob_obj1
# from lib import Label # cd in to Tai/daily_indicators
from numba import njit
from pymongo import MongoClient
import logging
import warnings
from itertools import product
from danglib.chatbots.viberbot import create_f5bot
from danglib.utils import write_pickle, walk_through_files
from tqdm import tqdm
from dc_server.lazy_core import gen_plasma_functions, maybe_create_dir
import ast

from danglib.utils import FileHandler

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
        self.df_stocks: pd.DataFrame = None
        self.days = None,
        self.function_map = {
            "price_change": Conds.price_change,
            "price_change_vs_hl": Conds.price_change_vs_hl,
            "price_comp_ma": Conds.price_comp_ma,
            "price_gap": Conds.price_gap,
            "price_highest_lowest": Conds.PriceAction.price_highest_lowest,
            "consecutive_conditional_bars": Conds.PriceAction.consecutive_conditional_bars,
            "vol_comp_ma": Conds.vol_comp_ma,
            "vol_percentile": Conds.vol_percentile,
            "relative_range" : Conds.relative_range,
            "consecutive_squeezes": Conds.consecutive_squeezes,
            "bbwp": Conds.bbwp,
            "bbwp2": Conds.bbwp2,
            "bbpctb": Conds.bbpctb,
            "ursi": Conds.ursi,
            "macd": Conds.macd,
            "index_cond": Conds.index_cond,
            "index_cond2": Conds.index_cond2,
            "lookback_cond": Conds.lookback_cond,
            "lookback_cond2": Conds.lookback_cond2,
            "lookback_cond3": Conds.lookback_cond3,

            'price_change_no_break': Conds.price_change_no_break_cond,

            "flow_percentile": Conds.Flow.percentile_cond,
            "flow_consecutive_bars": Conds.Flow.consecutive_bars,
            "flow_value" : Conds.Flow.value,
            "flow_two_mas" : Conds.Flow.two_mas,

            "returns_stock_vs_cluster_ranked": Conds.Cluster.returns_stock_vs_cluster_ranked,
            "returns_cluster_vs_index_ranked": Conds.Cluster.returns_cluster_vs_index_ranked,
            "returns_stock_vs_index_ranked": Conds.Cluster.returns_stock_vs_index_ranked,
            "consecutive_performance": Conds.Cluster.consecutive_performance,
            "returns_stock_vs_cluster_strength": Conds.Cluster.returns_stock_vs_cluster_strength,
            "returns_stock_vs_index_strength": Conds.Cluster.returns_stock_vs_index_strength,
            "returns_cluster_vs_index_strength": Conds.Cluster.returns_cluster_vs_index_strength,
            "volume_stock_vs_index" : Conds.Cluster.volume_stock_vs_index,
            "volume_cluster_vs_index" : Conds.Cluster.volume_cluster_vs_index,
            "volume_stock_vs_cluster" : Conds.Cluster.volume_stock_vs_cluster,
            "volume_cluster_vs_cluster_consecutive" : Conds.Cluster.volume_cluster_vs_cluster_consecutive,

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
            "custom_stocks_list": FilteringStocks.custom_stocks_list,
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
        df_stocks = Adapters.load_stocks_data_from_plasma()
        df_stocks = df_stocks[df_stocks['stock'].isin(glob_obj.stocks)]

        df_stocks = df_stocks.pivot(index='day', columns='stock')
        df_stocks['day'] = df_stocks.index


        self.df_stocks = df_stocks
        self.days = df_stocks['day'].to_list()

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
        # self.df_vnindex: pd.DataFrame = Adapters.get_stock_from_vnstock(
        #     "VNINDEX", from_day=self.data_from_day
        # )
        self.df_vnindex: pd.DataFrame = Adapters.get_vnindex_from_db()

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
            use_vs_hline: bool = False,
            hline: float = 0,
            hline_direction: str = "crossover"
        ):
            """Two line conditions"""
            pos_cond = Conds.Standards.two_line_pos(
                line1, line2, direction, use_flag=use_vs_signal
            )
            range_cond = Conds.Standards.range_cond(
                line1, lower_thres, upper_thres, use_flag=use_range
            )
            hline_series = pd.Series(hline, index = line1.index)
            hline_cond = Conds.Standards.two_line_pos(line1, hline_series, hline_direction, use_flag = use_vs_hline)
            res = Utils.combine_conditions([pos_cond, range_cond, hline_cond])
            return res
            
        @staticmethod
        def combined_strength_condition(
            df: pd.DataFrame,
            ma_use_flag: bool = False,
            ma_len1: int = 5,
            ma_len2: int = 15,
            ma_position: str = 'crossover',
            bb_use_flag: bool = False,
            bb_length: int = 20,
            bb_mult: float = 2,
            bb_position: str = 'crossover',
            bb_cross_line: str = 'upper',
            pct_use_flag: bool = False,
            pct_lookback: int = 128,
            pct_hline: float = 80,
            pct_hline_position: str = 'crossover',
        ):  
            res = None
            conds = []
            if ma_use_flag:
                df_ma1 = Ta.ma(df, ma_type = 'SMA', length = ma_len1)
                df_ma2 = Ta.ma(df, ma_type = 'SMA', length = ma_len2)
                ma_cond = Conds.Standards.two_line_pos(
                    line1=df_ma1,
                    line2=df_ma2,
                    direction=ma_position,
                    use_flag=ma_use_flag
                )
                if ma_cond is not None:
                    conds.append(ma_cond)
            if bb_use_flag:
                df_bb_basis = Ta.sma(df, bb_length)
                df_bb_stdev = Ta.stdev(df, bb_length)
                df_bb_upper = df_bb_basis + bb_mult * df_bb_stdev
                df_bb_lower = df_bb_basis - bb_mult * df_bb_stdev
                if bb_cross_line == 'upper':
                    df_bb_cross_line = df_bb_upper
                elif bb_cross_line == 'lower':
                    df_bb_cross_line = df_bb_lower 

                bb_cond = Conds.Standards.two_line_pos(
                    line1=df,
                    line2=df_bb_cross_line,
                    direction=bb_position,
                    use_flag=bb_use_flag
                )
                if bb_cond is not None:
                    conds.append(bb_cond)
            if pct_use_flag:
                df_pct = df.rolling(pct_lookback).rank()
                df_pct = (df_pct - 1) / (pct_lookback - 1) * 100
                hline = pd.DataFrame(pct_hline, index=df_pct.index, columns=df_pct.columns)
                pct_cond = Conds.Standards.two_line_pos(
                    line1=df_pct,
                    line2=hline,
                    direction= pct_hline_position,
                    use_flag=pct_use_flag
                )
                if pct_cond is not None:
                    conds.append(pct_cond)
            if len(conds) > 0:
                res = Utils.combine_conditions(conds)
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
            pd.DataFrame[bool]: True or False if use_flag is True else None
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
        high_range: float = 10000,
        wait_bars: bool = False,
        wbars: int = 5,
        compare_prev_peak_trough: bool = False,
        compare_prev_peak_trough_direction: str = "Higher",
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
            if wait_bars == False:
                return Utils.in_range(pct_change, low_range, high_range, equal=True)

            inrange = Utils.in_range(pct_change, low_range, high_range, equal=True)
            if direction == "Increase":
                mark = np.where(inrange, df['high'], np.nan)
                mark = pd.DataFrame(mark, index=close.index, columns= close.columns)
                hlofclose = Ta.highest(df['close'], wbars)
            else:
                mark = np.where(inrange, df['low'], np.nan)
                mark = pd.Series(mark, index=close.index, columns = close.columns)
                hlofclose = Ta.lowest(df['close'], wbars)
                
            fwmark = mark.shift(wbars)
            rs = Utils.new_1val_df(False, close) 
            if direction == "Increase":
                isw = hlofclose < fwmark
            else:
                isw = hlofclose > fwmark
            rs_arr = rs.values
            fwmark_arr = fwmark.values
            isw_arr = isw.values
            rs_arr[(~np.isnan(fwmark_arr) & isw_arr)] = True
            rs = pd.DataFrame(rs_arr, index = close.index, columns = close.columns)
            return rs
    
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
            # Calculating squeeze
            sqz_on, sqz_off, no_sqz  = Ta.squeeze(
                df, src_name, bb_length, length_kc, mult_kc, use_true_range
            )
            if use_no_sqz:
                return sqz_off
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
        use_vs_hline: bool = False,
        hline: float = 0,
        hline_direction: str = "crossover",
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
            use_vs_hline: bool = False
            hline: float = 0
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
                use_vs_hline,
                hline,
                hline_direction
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
    def relative_range(
        df: pd.DataFrame,
        smooth_len: int = 1,
        lower_thres: float = 0,
        upper_thres: float = 1,
        use_flag : bool = False
    ):        
        res = None
        if use_flag:
            relative_range = np.abs((df['close'] - df['open']) / (df['high'] - df['low'] + 0.001))
            smoothed_relative_range = relative_range.rolling(smooth_len).mean()
            res = Utils.in_range(smoothed_relative_range, lower_thres = lower_thres, upper_thres = upper_thres, equal=True)
        return res
    @staticmethod
    def index_cond(df: pd.DataFrame, function2, *args, **kwargs):
        """Compute index condition"""

        df2 = glob_obj.df_vnindex.copy()
        def test():
            df = df_raw.copy()
            function2 = 's_ursi'
            params = {

                'use_flag':True,
                'use_range': True,
                'lower_thres': 80
            }
            df2['cond']= Conds.compute_any_conds(df2, function2, **params)


        df2['cond']= Conds.compute_any_conds(df2, function2, *args, **kwargs)

        df2 = pd.merge(df['day'].reset_index(drop=True), df2, on='day', how='left' )
        df2['cond'] = df2['cond'].fillna(False)
        df2 = df2.set_index('day')  

        matched = df['close'].notna()
        matched = matched.apply(lambda col: col & df2['cond'])

        return matched
    
    @staticmethod
    def index_cond2(df: pd.DataFrame, **kwargs):
        df2 = glob_obj.df_vnindex.copy()

        df2['cond']= Conds.compute_any_conds2(df2, kwargs)

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
        return lookback_cond.rolling(5, closed = 'left').max().fillna(False).astype(bool)
    
    @staticmethod
    def lookback_cond2(df: pd.DataFrame, **kwargs):
        """Compute lookback condition"""
        lookback_cond: pd.DataFrame = Conds.compute_any_conds2(df, kwargs)

        return lookback_cond.rolling(5, closed = 'left').max().fillna(False).astype(bool)


    @staticmethod
    def lookback_cond3(df: pd.DataFrame, *args, **lookback_params: dict):
        n_bars = lookback_params.get('n_bars', 5)

        conds = []
        for func_name, func_params in lookback_params.items():
            try:
                if func_name == 'stock_scanner':
                    continue
                if(func_name not in glob_obj.function_map): 
                    continue
              
         
                func = glob_obj.function_map[func_name]
                cond: pd.DataFrame = func(df, **func_params)
           
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
    

    @staticmethod
    def detech_peak_by_price(df, pct_change, n_bars_before, n_bars_after):
        def test():
            df = df_raw.copy()
            pct_change = 10
            n_bars_before = 10
            n_bars_after = 5
            
        src: pd.DataFrame = df['close']

        df_price_change = Conds.price_change(df, 'close', periods=n_bars_before, lower_thres=pct_change, use_flag=True)

        df_rollmax = src.rolling(n_bars_after).max().shift(-(n_bars_after))
        df_matched_after = src >= df_rollmax
        
        df_matched = df_price_change & df_matched_after
        
        def test_res():
            view_stock = 'HPG'
            dfv = src[view_stock].to_frame('close')
            dfv['priceChange'] = dfv['close'].pct_change(periods=n_bars_before).round(6) * 100
            dfv['matchedPC'] = df_price_change[view_stock]
            dfv['rollmax'] = dfv['close'].rolling(n_bars_after).max()
            dfv['rollmaxShift'] = df_rollmax[view_stock]
            dfv['matchedAfter'] = df_matched_after[view_stock]
            dfv['matched'] = df_matched[view_stock]
            
            import plotly.graph_objects as go
            import pandas as pd

            df_ohlc = df.xs(view_stock, level = 'stock', axis=1)
            # Create a candlestick chart
            fig = go.Figure(data=[go.Candlestick(x=df_ohlc.index,
                            open=df_ohlc['open'],
                            high=df_ohlc['high'],
                            low=df_ohlc['low'],
                            close=df_ohlc['close'])])

            # Add titles and labels
            fig.update_layout(
                title='Candlestick Chart Example',
                xaxis_title='Date',
                yaxis_title='Price',
                xaxis_rangeslider_visible=False
            )

            # Show the figure
            fig.show()

    @staticmethod
    def price_change_no_break_cond(
        df: pd.DataFrame,
        src_name = 'close',
        use_flag: bool = False, 
        direction: str = "Increase",
        nbars: int = 10,
        low_range: float = 10,
        high_range: float = 10000,
        use_wait: bool = False,
        wbars: int = 5,
        bound: float = 3,
        ):
        nbars = int(nbars)
        close = df[src_name]
        comp_src = Ta.lowest(df['low'], nbars) if direction == 'Increase' else Ta.highest(df['high'], nbars)
        
        if use_flag:
            pct_change = Utils.calc_percentage_change(comp_src, close)
            if direction == "Decrease":
                pct_change = pct_change * -1
            if use_wait == False:
                return Utils.in_range(pct_change, low_range, high_range, equal=True)

            inrange = Utils.in_range(pct_change, low_range, high_range, equal=True)
            if direction == "Increase":
                mark = np.where(inrange, df['high'], np.nan)
                mark = pd.DataFrame(mark, index=close.index, columns= close.columns)
            else:
                mark = np.where(inrange, df['low'], np.nan)
                mark = pd.DataFrame(mark, index=close.index, columns = close.columns)

            highest_close = Ta.highest(df['close'], wbars)
            lowest_close = Ta.lowest(df['close'], wbars)
                
            fwmark = mark.shift(wbars)
            rs = Utils.new_1val_df(False, close) 
            isw = (highest_close < fwmark * (1 + bound / 100)) & (lowest_close > fwmark * (1 - bound / 100))
            rs_arr = rs.values
            fwmark_arr = fwmark.values
            isw_arr = isw.values
            rs_arr[(~np.isnan(fwmark_arr) & isw_arr)] = True
            rs = pd.DataFrame(rs_arr, index = close.index, columns = close.columns)
            return rs
    
        return None
    @staticmethod
    def ursi_no_break_cond(
        df: pd.DataFrame,
        use_flag: bool = False, 
        direction: str = "Increase",
        nbars: int = 10,
        low_range: float = 10,
        high_range: float = 10000,
        wait_bars: bool = False,
        wbars: int = 5,
        bound: float = 3,
        ):
        nbars = int(nbars)
        close = df['close']
        comp_src = Ta.lowest(df['low'], nbars) if direction == 'Increase' else Ta.highest(df['high'], nbars)
        ursi, _ = Ta.ursi(close)
        if use_flag:
            pct_change = Utils.calc_percentage_change(comp_src, close)
            if direction == "Decrease":
                pct_change = pct_change * -1
            if wait_bars == False:
                return Utils.in_range(pct_change, low_range, high_range, equal=True)

            inrange = Utils.in_range(pct_change, low_range, high_range, equal=True)
            if direction == "Increase":
                mark = np.where(inrange, df['high'], np.nan)
                mark = pd.DataFrame(mark, index=close.index, columns= close.columns)
            else:
                mark = np.where(inrange, df['low'], np.nan)
                mark = pd.Series(mark, index=close.index, columns = close.columns)

            highest_close = Ta.highest(df['close'], wbars)
            lowest_close = Ta.lowest(df['close'], wbars)
                
            fwmark = mark.shift(wbars)
            rs = Utils.new_1val_df(False, close) 
            isw = (highest_close < fwmark * (1 + bound / 100)) & (lowest_close > fwmark * (1 - bound / 100))
            rs_arr = rs.values
            fwmark_arr = fwmark.values
            isw_arr = isw.values
            rs_arr[(~np.isnan(fwmark_arr) & isw_arr)] = True
            rs = pd.DataFrame(rs_arr, index = close.index, columns = close.columns)
            return rs
    
        return None
    @staticmethod
    def wait_cond(df: pd.DataFrame,
                  entry_params: dict,
                  cfm_params: dict,
                  wait_bars: int = 5,
                  use_flag: bool = False):
        entry_signals = Conds.compute_any_conds(entry_params)
        def test_cfm_params():
            cfm_params = {
                'func_name':
                    {
                         'wait_bars' : 5
                    }
            }
        pass
    
    class Flow:
        @staticmethod
        def percentile_cond(
            df: pd.DataFrame,
            src_name: str = 'netBUSD',
            smoothing: int = 5,
            lookback: int = 128,
            position: str = 'crossover',
            threshold: float = 80,
            use_flag: bool = False
        ):
            """
            Check if the percentile of the source is within the specified range.
            src_name: netBUSD, netBUSD/Value, netForeign, netForeign/Value
            """
            res = None
            if use_flag:
                df_value = df['volume']
                df_bu = df['bu']
                df_sd = df['sd']
                df_netbusd = df_bu - df_sd
                df_netbusd_adjusted = df_netbusd / df_value

                df_fb = df['fBuyVal']
                df_fs = df['fSellVal']
                df_netforeign = df_fb - df_fs
                df_netforeign_adjusted = df_netforeign / df_value
                
                if src_name == 'netBUSD':
                    df_src = df_netbusd
                elif src_name == 'netBUSD/Value':
                    df_src = df_netbusd_adjusted
                elif src_name == 'netForeign':
                    df_src = df_netforeign
                elif src_name == 'netForeign/Value':
                    df_src = df_netforeign_adjusted
                df_src = df_src.rolling(smoothing).mean()
                df_src = df_src.rolling(lookback).rank()
                df_src = (df_src - 1) / (lookback - 1) * 100
                hline = pd.DataFrame(threshold, index = df_src.index, columns= df_src.columns)
                res = Conds.Standards.two_line_pos(
                    line1=df_src,
                    line2=hline,
                    direction=position,
                    use_flag=use_flag
                )
            return res
        @staticmethod
        def consecutive_bars(
            df: pd.DataFrame,
            src_name: str = 'netBUSD',
            smoothing: int = 5,
            direction: str = 'up',
            num_bars: int = 5,
            num_conditional_bars: int = 4,
            use_flag: bool = False
        ):
            """
            src_name: netBUSD, netBUSD/Value, netForeign, netForeign/Value
            direction: up, down, positive, negative
            """
            res = None
            if use_flag:
                df_value = df['volume']
                df_bu = df['bu']
                df_sd = df['sd']
                df_netbusd = df_bu - df_sd
                df_netbusd_adjusted = df_netbusd / df_value

                df_fb = df['fBuyVal']
                df_fs = df['fSellVal']
                df_netforeign = df_fb - df_fs
                df_netforeign_adjusted = df_netforeign / df_value
                
                if src_name == 'netBUSD':
                    df_src = df_netbusd
                elif src_name == 'netBUSD/Value':
                    df_src = df_netbusd_adjusted
                elif src_name == 'netForeign':
                    df_src = df_netforeign
                elif src_name == 'netForeign/Value':
                    df_src = df_netforeign_adjusted

                # Compute the rolling mean and rank
                smoothed_src = Ta.sma(df_src, smoothing)

                if direction == 'up':
                    df_cond = smoothed_src > smoothed_src.shift(1)
                elif direction == 'down':
                    df_cond = smoothed_src < smoothed_src.shift(1)
                elif direction == 'positive':
                    df_cond = smoothed_src > 0
                elif direction == 'negative':
                    df_cond = smoothed_src < 0

                df_cond = df_cond.rolling(num_bars).sum()
                res = df_cond >= num_conditional_bars

            return res
        @staticmethod
        def value(
            df: pd.DataFrame,
            src_name: str = 'netBUSD',
            smoothing: int = 5,
            position: str = 'crossover',
            threshold: float = 0,
            use_flag: bool = False
        ):
            """
            src_name: netBUSD, netBUSD/Value, netForeign, netForeign/Value
            position: crossover, crossunder, above, below
            """            
            res = None
            if use_flag:
                df_value = df['volume']
                df_bu = df['bu']
                df_sd = df['sd']
                df_netbusd = df_bu - df_sd
                df_netbusd_adjusted = df_netbusd / df_value

                df_fb = df['fBuyVal']
                df_fs = df['fSellVal']
                df_netforeign = df_fb - df_fs
                df_netforeign_adjusted = df_netforeign / df_value
                
                if src_name == 'netBUSD':
                    df_src = df_netbusd
                elif src_name == 'netBUSD/Value':
                    df_src = df_netbusd_adjusted
                elif src_name == 'netForeign':
                    df_src = df_netforeign
                elif src_name == 'netForeign/Value':
                    df_src = df_netforeign_adjusted

                smoothed_src = Ta.sma(df_src, smoothing)

                hline = pd.DataFrame(threshold, index=df_src.index, columns=df_src.columns)
                
                res = Conds.Standards.two_line_pos(
                    line1=smoothed_src,
                    line2=hline,
                    direction=position,
                    use_flag=use_flag
                )

            return res
        @staticmethod
        def two_mas(
            df: pd.DataFrame,
            src_name: str = 'netBUSD',
            ma_type: str = 'SMA',
            ma_len1: int = 5,
            ma_len2: int = 15,
            position: str = 'crossover',
            use_flag: bool = False
        ):
            """
            src_name: netBUSD, netBUSD/Value, netForeign, netForeign/Value
            ma_type: SMA, EMA
            position: crossover, crossunder, above, below
            """
            res = None
            if use_flag:
                df_value = df['volume']
                df_bu = df['bu']
                df_sd = df['sd']
                df_netbusd = df_bu - df_sd
                df_netbusd_adjusted = df_netbusd / df_value

                df_fb = df['fBuyVal']
                df_fs = df['fSellVal']
                df_netforeign = df_fb - df_fs
                df_netforeign_adjusted = df_netforeign / df_value
                
                if src_name == 'netBUSD':
                    df_src = df_netbusd
                elif src_name == 'netBUSD/Value':
                    df_src = df_netbusd_adjusted
                elif src_name == 'netForeign':
                    df_src = df_netforeign
                elif src_name == 'netForeign/Value':
                    df_src = df_netforeign_adjusted

                df_ma1 = Ta.ma(df_src, ma_type =ma_type,  length = ma_len1)
                df_ma2 = Ta.ma(df_src, ma_type =ma_type,  length = ma_len2)

                res = Conds.Standards.two_line_pos(
                    line1=df_ma1,
                    line2=df_ma2,
                    direction=position,
                    use_flag=use_flag
                )
            return res
    class Cluster:
        @staticmethod
        def returns_stock_vs_cluster_ranked(
            df: pd.DataFrame,
            num_bars: int = 1,
            top_or_bottom: str = 'top',
            ranking: int = 3,
            use_flag: bool = False
        ):
            """
            num_bars: number of bars to calculate return
            """
            res = None
            if use_flag:
                dic_groups_hrp = Adapters.get_hrp_group()
                df_return = df['close'].pct_change(periods=num_bars)
                if top_or_bottom == 'top':
                    df_ranking_vs_cluster = df_return.groupby(dic_groups_hrp, axis =1).rank(ascending = False, method = 'min')
                else:
                    df_ranking_vs_cluster = df_return.groupby(dic_groups_hrp, axis =1).rank(ascending = True, method = 'min')
                res = df_ranking_vs_cluster <= ranking
            return res
        @staticmethod
        def returns_stock_vs_index_ranked(
            df: pd.DataFrame,
            num_bars: int = 1,
            top_or_bottom: str = 'top',
            ranking: int = 3,
            use_flag: bool = False
        ):
            """
            num_bars: number of bars to calculate return
            """
            res = None
            if use_flag:
                df_return = df['close'].pct_change(periods=num_bars)
                if top_or_bottom == 'top':
                    df_ranking_vs_market = df_return.rank(ascending = False, method = 'min', axis =1)
                else:
                    df_ranking_vs_market = df_return.rank(ascending = True, method = 'min', axis =1)
                res = df_ranking_vs_market <= ranking
            return res
        @staticmethod
        def returns_cluster_vs_index_ranked(
            df: pd.DataFrame,
            num_bars: int = 1,
            top_or_bottom: str = 'top',
            ranking: int = 3,
            use_flag: bool = False
        ):
            """
            num_bars: number of bars to calculate return
            """
            res = None
            df_close = df['close']
            if use_flag:
                dic_groups_hrp = Adapters.get_hrp_group()
                cluster_stock_map = {}    
                for cluster in dic_groups_hrp.values():
                    cluster_stock_map[cluster] = [k for k,v in dic_groups_hrp.items() if v == cluster]
                s_cluster_map = pd.Series(cluster_stock_map)
                stock_map_df = s_cluster_map.explode().reset_index()
                stock_map_df.columns = ['cluster', 'stock']


                df_return = df['close'].pct_change(periods=num_bars)
                df_return_cluster = df_return.groupby(dic_groups_hrp, axis =1).mean()
                if top_or_bottom == 'top':
                    df_return_cluster_ranked = df_return_cluster.rank(ascending = False, method = 'min', axis =1)   
                else:
                    df_return_cluster_ranked = df_return_cluster.rank(ascending = True, method = 'min', axis =1)

                df_reset = df_return_cluster_ranked.reset_index()
                # Melt: Turn cluster columns into rows
                df_long = df_reset.melt(
                    id_vars='day',                  # Column(s) to keep as identifier variables
                    var_name='cluster',             # Name for the new column holding cluster names
                    value_name='cluster_value'      # Name for the new column holding the values
                )
                merged_df = pd.merge(df_long, stock_map_df, on='cluster')
                result_df = merged_df.pivot(index='day', columns='stock', values='cluster_value')
                result_df = result_df.sort_index(axis=1)  
                res = np.where(df_close.isna(), False, result_df <= ranking)
                res = pd.DataFrame(res, index=df_close.index, columns=df_close.columns)
            return res
        @staticmethod
        def consecutive_performance(
            df: pd.DataFrame,
            num_return_bars: int = 1,
            direction: str = 'positive',
            num_consecutive_bars: int = 5,
            num_conditional_bars: int = 4,
            use_flag: bool = False
        ):
            res = None
            if use_flag:
                df_return = df['close'].pct_change(periods=num_return_bars)
  
                df_vnindex = glob_obj1.get_one_stock_data('VNINDEX')
                df_vnindex_return = df_vnindex['close'].pct_change(periods=num_return_bars) 
                df_vnindex_return.index = df_vnindex['day']
                df_vnindex_return = df_vnindex_return.reindex(df_return.index) # de khong loi khi data bi miss
                df_relative_return = df_return.subtract(df_vnindex_return, axis = 0)
                if direction == 'positive':
                    df_cond = df_relative_return > 0
                elif direction == 'negative':
                    df_cond = df_relative_return < 0
                df_cond = df_cond.rolling(num_consecutive_bars).sum()  

                res = df_cond >= num_conditional_bars
            return res

        @staticmethod
        def returns_stock_vs_cluster_strength(
            df: pd.DataFrame,
            num_return_bars: int = 1,
            ma_use_flag: bool = False,
            ma_len1: int = 5,
            ma_len2: int = 15,
            ma_position: str = 'crossover',
            bb_use_flag: bool = False,
            bb_length: int = 20,
            bb_mult: float = 2,
            bb_position: str = 'crossover',
            bb_cross_line: str = 'upper',
            pct_use_flag: bool = False,
            pct_lookback: int = 128,
            pct_hline: float = 80,
            pct_hline_position: str = 'crossover',
            use_flag: bool = False
        ):
            res = None
            if use_flag:
                df_returns = df['close'].pct_change(periods=num_return_bars) * 100

                dic_groups_hrp = Adapters.get_hrp_group()
                df_relative_returns = pd.DataFrame()
                for group_name in set(dic_groups_hrp.values()):
                    group_columns = [col for col, group in dic_groups_hrp.items() if group == group_name]
                    if group_columns:
                        # Select the columns for the current group
                        group_data = df_returns[group_columns]

                        group_mean = group_data.mean(axis=1)

                        normalized_group_data = group_data.subtract(group_mean, axis=0)

                        # Add the normalized columns to the output DataFrame
                        df_relative_returns = pd.concat([df_relative_returns, normalized_group_data], axis=1)
                df_relative_returns = df_relative_returns.sort_index(axis=1)
                res = Conds.Standards.combined_strength_condition(df_relative_returns,
                                                                  ma_use_flag=ma_use_flag,
                                                                ma_len1=ma_len1,
                                                                ma_len2=ma_len2,
                                                                ma_position=ma_position,
                                                                bb_use_flag=bb_use_flag,
                                                                bb_length=bb_length,
                                                                bb_mult=bb_mult,
                                                                bb_position=bb_position,
                                                                bb_cross_line=bb_cross_line,
                                                                pct_use_flag=pct_use_flag,
                                                                pct_lookback=pct_lookback,
                                                                pct_hline=pct_hline,
                                                                pct_hline_position=pct_hline_position
                )
            return res
        @staticmethod
        def returns_stock_vs_index_strength(
            df: pd.DataFrame,
            num_return_bars: int = 1,
            ma_use_flag: bool = False,
            ma_len1: int = 5,
            ma_len2: int = 15,
            ma_position: str = 'crossover',
            bb_use_flag: bool = False,
            bb_length: int = 20,
            bb_mult: float = 2,
            bb_position: str = 'crossover',
            bb_cross_line: str = 'upper',
            pct_use_flag: bool = False,
            pct_lookback: int = 128,
            pct_hline: float = 80,
            pct_hline_position: str = 'crossover',
            use_flag: bool = False
        ):
            res = None
            if use_flag:
                df_returns = df['close'].pct_change(periods=num_return_bars) * 100
                df_vnindex = glob_obj1.get_one_stock_data('VNINDEX')
                df_vnindex_return = df_vnindex['close'].pct_change(periods=num_return_bars) * 100
                df_vnindex_return.index = df_vnindex['day']
                df_vnindex_return = df_vnindex_return.reindex(df_returns.index) # de khong loi khi data bi miss
                # Calculate the relative returns
                df_relative_returns = df_returns.subtract(df_vnindex_return, axis=0)
               
                df_relative_returns = df_relative_returns.sort_index(axis=1)
                res = Conds.Standards.combined_strength_condition(df_relative_returns,
                                                                  ma_use_flag=ma_use_flag,
                                                                ma_len1=ma_len1,
                                                                ma_len2=ma_len2,
                                                                ma_position=ma_position,
                                                                bb_use_flag=bb_use_flag,
                                                                bb_length=bb_length,
                                                                bb_mult=bb_mult,
                                                                bb_position=bb_position,
                                                                bb_cross_line=bb_cross_line,
                                                                pct_use_flag=pct_use_flag,
                                                                pct_lookback=pct_lookback,
                                                                pct_hline=pct_hline,
                                                                pct_hline_position=pct_hline_position
                )
            return res
        @staticmethod
        def returns_cluster_vs_index_strength(
            df: pd.DataFrame,
            num_return_bars: int = 1,
            ma_use_flag: bool = False,
            ma_len1: int = 5,
            ma_len2: int = 15,
            ma_position: str = 'crossover',
            bb_use_flag: bool = False,
            bb_length: int = 20,
            bb_mult: float = 2,
            bb_position: str = 'crossover',
            bb_cross_line: str = 'upper',
            pct_use_flag: bool = False,
            pct_lookback: int = 128,
            pct_hline: float = 80,
            pct_hline_position: str = 'crossover',
            use_flag: bool = False
        ):
            res = None
            if use_flag:
                df_returns = df['close'].pct_change(periods=num_return_bars) * 100
                df_vnindex = glob_obj1.get_one_stock_data('VNINDEX')
                df_vnindex_return = df_vnindex['close'].pct_change(periods=num_return_bars) * 100
                df_vnindex_return.index = df_vnindex['day']
                df_vnindex_return = df_vnindex_return.reindex(df_returns.index) # de khong loi khi data bi miss

                dic_groups_hrp = Adapters.get_hrp_group()
                cluster_stock_map = {}    
                for cluster in dic_groups_hrp.values():
                    cluster_stock_map[cluster] = [k for k,v in dic_groups_hrp.items() if v == cluster]
                s_cluster_map = pd.Series(cluster_stock_map)
                stock_map_df = s_cluster_map.explode().reset_index()
                stock_map_df.columns = ['cluster', 'stock']

                # df_returns_index = df_returns.mean(axis=1)
                df_returns_cluster = df_returns.groupby(dic_groups_hrp, axis =1).mean()
                df_relative_volume_cluster_index = df_returns_cluster.subtract(df_vnindex_return, axis=0)
                
                df_reset = df_relative_volume_cluster_index.reset_index()
                # Melt: Turn cluster columns into rows
                df_long = df_reset.melt(
                    id_vars='day',                  # Column(s) to keep as identifier variables
                    var_name='cluster',             # Name for the new column holding cluster names
                    value_name='cluster_value'      # Name for the new column holding the values
                )
                merged_df = pd.merge(df_long, stock_map_df, on='cluster')
                df_relative_returns = merged_df.pivot(index='day', columns='stock', values='cluster_value')
                df_relative_returns = df_relative_returns.sort_index(axis=1) 
                df_relative_returns = np.where(df_returns.isna(), np.nan, df_relative_returns)
                df_relative_returns = pd.DataFrame(df_relative_returns, index=df_returns.index, columns=df_returns.columns)
          
                df_relative_returns = df_relative_returns.sort_index(axis=1)
                res = Conds.Standards.combined_strength_condition(df_relative_returns,
                                                                  ma_use_flag=ma_use_flag,
                                                                ma_len1=ma_len1,
                                                                ma_len2=ma_len2,
                                                                ma_position=ma_position,
                                                                bb_use_flag=bb_use_flag,
                                                                bb_length=bb_length,
                                                                bb_mult=bb_mult,
                                                                bb_position=bb_position,
                                                                bb_cross_line=bb_cross_line,
                                                                pct_use_flag=pct_use_flag,
                                                                pct_lookback=pct_lookback,
                                                                pct_hline=pct_hline,
                                                                pct_hline_position=pct_hline_position
                )
            return res
        @staticmethod
        def volume_stock_vs_index(
            df: pd.DataFrame,
            smoothing: int = 5,
            ma_use_flag: bool = False,
            ma_len1: int = 5,
            ma_len2: int = 15,
            ma_position: str = 'crossover',
            bb_use_flag: bool = False,
            bb_length: int = 20,
            bb_mult: float = 2,
            bb_position: str = 'crossover',
            bb_cross_line: str = 'upper',
            pct_use_flag: bool = False,
            pct_lookback: int = 128,
            pct_hline: float = 80,
            pct_hline_position: str = 'crossover',
            use_flag: bool = False
        ):
      
            res = None
            if use_flag:
                df_volume = df['volume'].rolling(smoothing).mean()
                df_volume_index = df_volume.sum(axis=1)
                df_relative_volume = df_volume.div(df_volume_index, axis=0) * 100
                res = Conds.Standards.combined_strength_condition(df_relative_volume,
                                                                ma_use_flag=ma_use_flag,
                                                                ma_len1=ma_len1,
                                                                ma_len2=ma_len2,
                                                                ma_position=ma_position,
                                                                bb_use_flag=bb_use_flag,
                                                                bb_length=bb_length,
                                                                bb_mult=bb_mult,
                                                                bb_position=bb_position,
                                                                bb_cross_line=bb_cross_line,
                                                                pct_use_flag=pct_use_flag,
                                                                pct_lookback=pct_lookback,
                                                                pct_hline=pct_hline,
                                                                pct_hline_position=pct_hline_position
                )
                # if ma_use_flag:
                #     df_ma1 = Ta.ma(df_relative_volume, ma_type = 'SMA', length = ma_len1)
                #     df_ma2 = Ta.ma(df_relative_volume, ma_type = 'SMA', length = ma_len2)
                #     res = Conds.Standards.two_line_pos(
                #         line1=df_ma1,
                #         line2=df_ma2,
                #         direction=ma_position,
                #         use_flag=ma_use_flag
                #     )
                # elif bb_use_flag:
                #     df_bb_basis = Ta.sma(df_relative_volume, bb_length)
                #     df_bb_stdev = Ta.stdev(df_relative_volume, bb_length)
                #     df_bb_upper = df_bb_basis + bb_mult * df_bb_stdev
                #     df_bb_lower = df_bb_basis - bb_mult * df_bb_stdev
                #     if bb_cross_line == 'upper':
                #         df_bb_cross_line = df_bb_upper
                #     elif bb_cross_line == 'lower':
                #         df_bb_cross_line = df_bb_lower 

                #     res = Conds.Standards.two_line_pos(
                #         line1=df_relative_volume,
                #         line2=df_bb_cross_line,
                #         direction=bb_position,
                #         use_flag=bb_use_flag
                #     )
                # elif pct_use_flag:
                #     df_pct = df_relative_volume.rolling(pct_lookback).rank()
                #     df_pct = (df_pct - 1) / (pct_lookback - 1) * 100
                #     hline = pd.DataFrame(pct_hline, index=df_pct.index, columns=df_pct.columns)
                #     res = Conds.Standards.two_line_pos(
                #         line1=df_pct,
                #         line2=hline,
                #         direction= pct_hline_position,
                #         use_flag=pct_use_flag
                #     )
            return res
        

        @staticmethod
        def volume_stock_vs_cluster(
            df: pd.DataFrame,
            smoothing: int = 5,
            ma_use_flag: bool = False,
            ma_len1: int = 5,
            ma_len2: int = 15,
            ma_position: str = 'crossover',
            bb_use_flag: bool = False,
            bb_length: int = 20,
            bb_mult: float = 2,
            bb_position: str = 'crossover',
            bb_cross_line: str = 'upper',
            pct_use_flag: bool = False,
            pct_lookback: int = 128,
            pct_hline: float = 80,
            pct_hline_position: str = 'crossover',
            use_flag: bool = False
        ):
            res = None
            if use_flag:
                df_volume = df['volume'].rolling(smoothing).mean()

                dic_groups_hrp = Adapters.get_hrp_group()
                df_relative_volume = pd.DataFrame()
                for group_name in set(dic_groups_hrp.values()):
                    group_columns = [col for col, group in dic_groups_hrp.items() if group == group_name]
                    if group_columns:
                        # Select the columns for the current group
                        group_data = df_volume[group_columns]

                        # Calculate the sum of the columns in the current group for each row
                        group_sum = group_data.sum(axis=1)

                        # Divide each column in the group by the group sum
                        normalized_group_data = group_data.div(group_sum, axis=0) * 100

                        # Add the normalized columns to the output DataFrame
                        df_relative_volume = pd.concat([df_relative_volume, normalized_group_data], axis=1)
                df_relative_volume = df_relative_volume.sort_index(axis=1)
                res = Conds.Standards.combined_strength_condition(df_relative_volume,
                                                                ma_use_flag=ma_use_flag,
                                                                ma_len1=ma_len1,
                                                                ma_len2=ma_len2,
                                                                ma_position=ma_position,
                                                                bb_use_flag=bb_use_flag,
                                                                bb_length=bb_length,
                                                                bb_mult=bb_mult,
                                                                bb_position=bb_position,
                                                                bb_cross_line=bb_cross_line,
                                                                pct_use_flag=pct_use_flag,
                                                                pct_lookback=pct_lookback,
                                                                pct_hline=pct_hline,
                                                                pct_hline_position=pct_hline_position
                )
                # if ma_use_flag:
                #     df_ma1 = Ta.ma(df_relative_volume, ma_type = 'SMA', length = ma_len1)
                #     df_ma2 = Ta.ma(df_relative_volume, ma_type = 'SMA', length = ma_len2)
                #     res = Conds.Standards.two_line_pos(
                #         line1=df_ma1,
                #         line2=df_ma2,
                #         direction=ma_position,
                #         use_flag=ma_use_flag
                #     )
                # elif bb_use_flag:
                #     df_bb_basis = Ta.sma(df_relative_volume, bb_length)
                #     df_bb_stdev = Ta.stdev(df_relative_volume, bb_length)
                #     df_bb_upper = df_bb_basis + bb_mult * df_bb_stdev
                #     df_bb_lower = df_bb_basis - bb_mult * df_bb_stdev
                #     if bb_cross_line == 'upper':
                #         df_bb_cross_line = df_bb_upper
                #     elif bb_cross_line == 'lower':
                #         df_bb_cross_line = df_bb_lower 

                #     res = Conds.Standards.two_line_pos(
                #         line1=df_relative_volume,
                #         line2=df_bb_cross_line,
                #         direction=bb_position,
                #         use_flag=bb_use_flag
                #     )
                # elif pct_use_flag:
                #     df_pct = df_relative_volume.rolling(pct_lookback).rank()
                #     df_pct = (df_pct - 1) / (pct_lookback - 1) * 100
                #     hline = pd.DataFrame(pct_hline, index=df_pct.index, columns=df_pct.columns)
                #     res = Conds.Standards.two_line_pos(
                #         line1=df_pct,
                #         line2=hline,
                #         direction= pct_hline_position,
                #         use_flag=pct_use_flag
                #     )
            return res
        
        @staticmethod
        def volume_cluster_vs_index(
            df: pd.DataFrame,
            smoothing: int = 5,
            ma_use_flag: bool = False,
            ma_len1: int = 5,
            ma_len2: int = 15,
            ma_position: str = 'crossover',
            bb_use_flag: bool = False,
            bb_length: int = 20,
            bb_mult: float = 2,
            bb_position: str = 'crossover',
            bb_cross_line: str = 'upper',
            pct_use_flag: bool = False,
            pct_lookback: int = 128,
            pct_hline: float = 80,
            pct_hline_position: str = 'crossover',
            use_flag: bool = False
        ):
            res = None
            if use_flag:
                df_volume = df['volume'].rolling(smoothing).mean()
                dic_groups_hrp = Adapters.get_hrp_group()
                cluster_stock_map = {}    
                for cluster in dic_groups_hrp.values():
                    cluster_stock_map[cluster] = [k for k,v in dic_groups_hrp.items() if v == cluster]
                s_cluster_map = pd.Series(cluster_stock_map)
                stock_map_df = s_cluster_map.explode().reset_index()
                stock_map_df.columns = ['cluster', 'stock']

                df_volume_index = df_volume.sum(axis=1)
                df_volume_cluster = df_volume.groupby(dic_groups_hrp, axis =1).sum()
                df_relative_volume_cluster_index = df_volume_cluster.div(df_volume_index, axis=0) * 100
                
                df_reset = df_relative_volume_cluster_index.reset_index()
                # Melt: Turn cluster columns into rows
                df_long = df_reset.melt(
                    id_vars='day',                  # Column(s) to keep as identifier variables
                    var_name='cluster',             # Name for the new column holding cluster names
                    value_name='cluster_value'      # Name for the new column holding the values
                )
                merged_df = pd.merge(df_long, stock_map_df, on='cluster')
                df_relative_volume = merged_df.pivot(index='day', columns='stock', values='cluster_value')
                df_relative_volume = df_relative_volume.sort_index(axis=1) 
                df_relative_volume = np.where(df_volume.isna(), np.nan, df_relative_volume)
                df_relative_volume = pd.DataFrame(df_relative_volume, index=df_volume.index, columns=df_volume.columns)

                res = Conds.Standards.combined_strength_condition(df_relative_volume,
                                                                ma_use_flag=ma_use_flag,
                                                                ma_len1=ma_len1,
                                                                ma_len2=ma_len2,
                                                                ma_position=ma_position,
                                                                bb_use_flag=bb_use_flag,
                                                                bb_length=bb_length,
                                                                bb_mult=bb_mult,
                                                                bb_position=bb_position,
                                                                bb_cross_line=bb_cross_line,
                                                                pct_use_flag=pct_use_flag,
                                                                pct_lookback=pct_lookback,
                                                                pct_hline=pct_hline,
                                                                pct_hline_position=pct_hline_position
                )
               
            return res
        @staticmethod
        def volume_cluster_vs_cluster_consecutive(
            df: pd.DataFrame,
            smoothing: int = 5,
            num_bars: int = 5,
            top_or_bottom: str = 'top',
            ranking: int = 3,
            consecutive_bars: int = 5,
            num_conditional_bars: int = 4,
            use_flag: bool = False
        ):
            res = None
            if use_flag:
                df_volume = df['volume'].rolling(smoothing).mean()
                dic_groups_hrp = Adapters.get_hrp_group()
                cluster_stock_map = {}    
                for cluster in dic_groups_hrp.values():
                    cluster_stock_map[cluster] = [k for k,v in dic_groups_hrp.items() if v == cluster]
                s_cluster_map = pd.Series(cluster_stock_map)
                stock_map_df = s_cluster_map.explode().reset_index()
                stock_map_df.columns = ['cluster', 'stock']

                df_volume_index = df_volume.sum(axis=1)
                df_volume_cluster = df_volume.groupby(dic_groups_hrp, axis =1).sum()
                df_relative_volume_cluster_index = df_volume_cluster.div(df_volume_index, axis=0) * 100
                df_relative_volume_cluster_index = df_relative_volume_cluster_index.pct_change(periods=num_bars) * 100
                if top_or_bottom == 'top':
                   df_relative_volume_cluster_index = df_relative_volume_cluster_index.rank(ascending = False, method = 'min', axis =1)
                else:
                   df_relative_volume_cluster_index = df_relative_volume_cluster_index.rank(ascending = True, method = 'min', axis =1)

                df_reset = df_relative_volume_cluster_index.reset_index()
                # Melt: Turn cluster columns into rows
                df_long = df_reset.melt(
                    id_vars='day',                  # Column(s) to keep as identifier variables
                    var_name='cluster',             # Name for the new column holding cluster names
                    value_name='cluster_value'      # Name for the new column holding the values
                )
                merged_df = pd.merge(df_long, stock_map_df, on='cluster')
                df_relative_volume = merged_df.pivot(index='day', columns='stock', values='cluster_value')
                df_relative_volume = df_relative_volume.sort_index(axis=1)
                df_relative_volume = np.where(df_volume.isna(), np.nan, df_relative_volume)
                df_relative_volume = pd.DataFrame(df_relative_volume, index=df_volume.index, columns=df_volume.columns)

                df_cond = df_relative_volume <= ranking
                df_cond = df_cond.rolling(consecutive_bars).sum()
                res = df_cond >= num_conditional_bars
            return res

class FilteringStocks:

    @staticmethod
    def filter_sector(
        values: list,
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
    def custom_stocks_list(stocks, use_flag: bool= False):
        def test():
            stocks = "'AGR', 'HOSE:ANV', 'HOSE:ASM', 'HOSE:BSI'"

            stocks = ast.literal_eval(stocks)


        if use_flag:
            if isinstance(stocks, str):
                stocks = ast.literal_eval(stocks)

            res_ls = []
            for s in stocks:
                if ':' in s:
                    res_ls.append(s.split(':')[1])
                else:
                    res_ls.append(s)

            return res_ls
        
        return None
        # @staticmethod
    # def filter_ta_fa(day, use_flag = False, **functions_params_dic):
    #     def test():
    #         functions_params_dic={
    #             'index_cond2': {
    #                 'price_change':{
    #                     'use_flag': True,
    #                     'lower_thres': 0.5
    #                 }
    #             }
    #         }
    #     if use_flag:
    #         df = glob_obj.df_stocks

    #         matched: pd.Series = Conds.compute_any_conds2(df, functions_params_dic).loc[day]
    #         res = matched[matched == True]
    #         return list(res.index)
        
    #     return None
    
    @staticmethod
    def filter_all(filter_functions_params_dic: dict, day=None):
        def test():
            Conds.FA
            filter_functions_params_dic = {
                'net_income': {
                    'values': ['Medium'],
                    'use_flag': True
                }
            }
            day = '2024_08_09'



        stocks_list = glob_obj.stocks
        for func_name, params in filter_functions_params_dic.items():
            try:
                if func_name == 'stock_scanner':
                    continue
                if(func_name not in glob_obj.function_map): 
                    continue
                func = glob_obj.function_map[func_name]
                if "FilteringStocks" in str(func):
                    tmp = func(**params)
                else:
                    df = glob_obj.df_stocks
                    tmp = func(df, **params)
                    if tmp is not None:
                        tmp = tmp.loc[day]
                        tmp = list(tmp[tmp==True].index)

                if tmp is not None:
                    stocks_list = list(set(stocks_list).intersection(tmp))

            except Exception as e:
                logging.error(f"function `filter_all` error: {func_name}: {e}", exc_info=True)

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
                 },
                 'type': 'trigger'
            },
            'price2':{
                'function':'price_change',
                'params' : {
                    'periods': [2],
                    'lower_thres': [6, 10, 13],
                    'direction': ['increase', 'decrease']
                },
                'type': 'trigger'
            }, 
            'price3':{
                'function': 'price_change',
                'params' : {
                    'periods': [3],
                    'lower_thres': [9, 15, 19],
                    'direction': ['increase', 'decrease']
                },
                'type': 'trigger'
            },
            'price4':{
                'function': 'price_change',
                'params' : {
                    'periods': [5],
                    'lower_thres': [10, 15, 20, 25],
                    'direction': ['increase', 'decrease']
                },
                'type': 'trigger'
            },
            'price_ma1_env':{
                'function': 'price_comp_ma',
                'params': {
                    'ma_len1':[1],
                    'ma_len2':[5,10,15,20,50,200],
                    'ma_dir': ['above', 'below'],
                },
                'type': 'env'
            },
            'price_ma1_trigger':{
                'function': 'price_comp_ma',
                'params': {
                    'ma_len1':[1],
                    'ma_len2':[5,10,15,20,50,200],
                    'ma_dir': ['crossover', 'crossunder'],
                },
                'type': 'trigger'
            },
            'price_ma2_env':{
                'function': 'price_comp_ma',
                'params': {
                    'ma_len1':[5],
                    'ma_len2':[15,20],
                    'ma_dir': ['above', 'below'],
                },
                'type': 'env'
            },
            'price_ma2_trigger':{
                'function': 'price_comp_ma',
                'params': {
                    'ma_len1':[5],
                    'ma_len2':[15,20],
                    'ma_dir': ['crossover', 'crossunder']
                },
                'type': 'trigger'
            },
            'price_ma3_env':{
                'function': 'price_comp_ma',
                'params': {
                    'ma_len1':[10],
                    'ma_len2':[30],
                    'ma_dir': ['above', 'below',],
                },
                'type': 'env'
            },
            'price_ma3_trigger':{
                'function': 'price_comp_ma',
                'params': {
                    'ma_len1':[10],
                    'ma_len2':[30],
                    'ma_dir': ['crossover', 'crossunder'],
                },
                'type': 'trigger'
            },
            'price_ma4_env':{
                'function': 'price_comp_ma',
                'params': {
                    'ma_len1':[20],
                    'ma_len2':[50, 100],
                    'ma_dir': ['above', 'below'],
                },
                'type':'env'
            },
            'price_ma4_trigger':{
                'function': 'price_comp_ma',
                'params': {
                    'ma_len1':[20],
                    'ma_len2':[50, 100],
                    'ma_dir': ['crossover', 'crossunder'],
                },
                'type':'trigger'
            },
            'price_ma5_env':{
                'function': 'price_comp_ma',
                'params': {
                    'ma_len1':[50],
                    'ma_len2':[200],
                    'ma_dir': ['above', 'below'],
                },
                'type':'env'
            },
            'price_ma5_trigger':{
                'function': 'price_comp_ma',
                'params': {
                    'ma_len1':[50],
                    'ma_len2':[200],
                    'ma_dir': ['crossover', 'crossunder'],
                },
                'type':'trigger'
            },
            'price_hl1':{
                'function': 'price_change_vs_hl',
                'params': {
                    'direction': ['Increase','Decrease'],
                    'nbars': [5, 10, 15, 20, 30],
                    'low_range': [10, 15, 20, 25]
                },
                'type': 'env'
            },
            'price_gap1':{
                'function': 'price_gap',
                'params':{
                    'gap_dir': ['Use Gap Up', 'Use Gap Down']
                },
                'type': 'trigger'
            },
            'hlest1':{
                'function': 'price_highest_lowest',
                'params': {
                    'method': ['Highest', 'Lowest'],
                    'num_bars': [5, 10, 15, 20, 50, 200]
                },
                'type': 'trigger'
            },
            'cons_bar1':{
                'function': 'consecutive_conditional_bars',
                'params': {
                    ('src1_name', 'src2_name'): [('close', 'close'), ('high', 'low'), ('low', 'low'), ('high', 'high'), ('high', 'close'), ('low', 'close')],
                    'direction': ['Increase', 'Decrease'],
                    ('num_bars', 'num_matched'): [(2,2), (3,3), (4,4), (5,5), (6,6), (7,7), (8,8)]
                },
                'type' : 'trigger'
            },
            'vol_comp_ma1':{
                'function': 'vol_comp_ma',
                'params' : {
                    ("n_bars", "ma_len") : [(1, 20), (1, 10), (1, 6), (2, 20), (3, 15)],
                    "comp_ma_dir": ["higher", "lower"],
                    "comp_ma_perc": [0, 25, 50, 75, 100, 150]
                },
                'type': 'trigger'
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
            'vol_perc2_env':{
                'function': 'vol_percentile',
                'params': {
                    'ma_length': [1,3,5,10],
                    'ranking_window': [64, 128, 256],
                    'low_range': [0],
                    'high_range': [10, 15, 20]
                },
                'type': 'env'
            },
            'vol_perc2_trigger':{
                'function': 'vol_percentile',
                'params': {
                    'ma_length': [1,3,5,10],
                    'ranking_window': [64, 128, 256],
                    'low_range': [0],
                    'high_range': [2,5]
                },
                'type': 'trigger'
            },
            'vol_perc3_env':{
                'function': 'vol_percentile',
                'params': {
                    'ma_length': [1,3,5,10],
                    'ranking_window': [64, 128, 256],
                    'low_range': [80, 90],
                    'high_range': [100]
                },
                'type': 'env'
            },
            'vol_perc3_trigger':{
                'function': 'vol_percentile',
                'params': {
                    'ma_length': [1,3,5,10],
                    'ranking_window': [64, 128, 256],
                    'low_range': [95, 98],
                    'high_range': [100]
                },
                'type': 'trigger'
            },
            'squeeze1':{
                'function': 'consecutive_squeezes', 
                'params': {
                    'use_no_sqz': [True]
                },
                'type': 'env'
            },
            'squeeze2':{
                'function': 'consecutive_squeezes', 
                'params': {
                    'num_bars': [3, 5, 10, 15, 20]
                },
                'type': 'env'
            },
            # 'bbwp1' : {
            #     'function': 'bbwp',
            #     'params': {
            #         ('use_low_thres', 'use_high_thres'):[(True, False), (False, True)],
            #         'bbwp_len': [13, 30, 50],
            #         'bbwp_lkbk': [64, 128, 256],
            #         'low_thres': [2,5,10,20],
            #         'high_thres': [80, 90, 95, 98]
            #     }
            # },
            'bbwp2_env' : {
                'function': 'bbwp',
                'params': {
                    ('use_low_thres', 'use_high_thres'):[(True, False)],
                    'bbwp_len': [13, 30, 50],
                    'bbwp_lkbk': [64, 128, 256],
                    'low_thres': [10,20],
                    'high_thres': [100]
                },
                'type': 'env'
            },
            'bbwp2_trigger' : {
                'function': 'bbwp',
                'params': {
                    ('use_low_thres', 'use_high_thres'):[(True, False)],
                    'bbwp_len': [13, 30, 50],
                    'bbwp_lkbk': [64, 128, 256],
                    'low_thres': [2,5],
                    'high_thres': [100]
                },
                'type': 'trigger'
            },
            'bbwp3_env' : {
                'function': 'bbwp',
                'params': {
                    ('use_low_thres', 'use_high_thres'):[(False, True)],
                    'bbwp_len': [13, 30, 50],
                    'bbwp_lkbk': [64, 128, 256],
                    'low_thres': [0],
                    'high_thres': [80, 90]
                },
                'type': 'env'
            },
            'bbwp3_trigger' : {
                'function': 'bbwp',
                'params': {
                    ('use_low_thres', 'use_high_thres'):[(False, True)],
                    'bbwp_len': [13, 30, 50],
                    'bbwp_lkbk': [64, 128, 256],
                    'low_thres': [0],
                    'high_thres': [95, 98]
                },
                'type': 'trigger'
            },
            'bbpctb1':{
                'function': 'bbpctb',
                'params':{
                'length': [20, 50, 100],
                'mult': [1.5, 2, 2.5, 3],
                'direction' : ['crossover', 'crossunder'],
                'cross_line': ['Upper band', 'Lower band']
                },
                'type': 'trigger'
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
            'ursi2_env': {
                'function': 'ursi',
                'params': {
                    'direction': ['above', 'below'],
                    ('use_vs_signal','use_range'): [(True, False), (False,True), (True, True)],
                    ('lower_thres', 'upper_thres'): [(0, 10), (0, 20), (10, 20), (20, 30), (80, 90), (90, 100), (80, 100)]
                },
                'type':'env'
            },
            'ursi2_trigger': {
                'function': 'ursi',
                'params': {
                    'direction': ['crossover', 'crossunder'],
                    ('use_vs_signal','use_range'): [(True, False), (True, True)],
                    ('lower_thres', 'upper_thres'): [(0, 10), (0, 20), (10, 20), (20, 30), (80, 90), (90, 100), (80, 100)]
                },
                'type':'trigger'
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
            'macd2_env': {
                'function': 'macd',
                'params': {
                    ('use_vs_signal','use_range'): [(False,True)],
                    'direction': ['crossover', 'crossunder'],
                    ('lower_thres','upper_thres'):[(-9999999,0), (0, 9999999)]
                },
                'type': 'env'
            },
            'macd2_trigger': {
                'function': 'macd',
                'params': {
                    ('use_vs_signal','use_range'): [(True, False), (True, True)],
                    'direction': ['crossover', 'crossunder'],
                    ('lower_thres','upper_thres'):[(-9999999,0), (0, 9999999)]
                },
                'type': 'trigger'
            },
            'index1_env': {
                'function': 'index_cond',
                'params': {
                    'function2': ['s_price_comp_ma'],
                    ('ma_len1', 'ma_len2'): [(1,5), (1, 10), (1, 15), (1, 20), (1, 50), (1, 200), (5, 15), (5, 20), (10, 30), (20, 50), (20, 100), (50, 200)],
                    'ma_dir': ['above', 'below']
                },
                'type': 'env'
            },
            'index1_trigger': {
                'function': 'index_cond',
                'params': {
                    'function2': ['s_price_comp_ma'],
                    ('ma_len1', 'ma_len2'): [(1,5), (1, 10), (1, 15), (1, 20), (1, 50), (1, 200), (5, 15), (5, 20), (10, 30), (20, 50), (20, 100), (50, 200)],
                    'ma_dir': ['crossover', 'crossunder']
                },
                'type': 'trigger'
            },
            # 'index2': {
            #     'function': 'index_cond',
            #     'params': {
            #         'function2': ['s_bbwp'],
            #         ('use_low_thres', 'use_high_thres'):[(True, False), (False, True)],
            #         'bbwp_len': [13, 30, 50],
            #         'bbwp_lkbk': [64, 128, 256],
            #         'low_thres': [2,5,10,20],
            #         'high_thres': [80, 90, 95, 98]
            #     }
            # },
            'index2.1': {
                'function': 'index_cond',
                'params': {
                    'function2': ['s_bbwp'],
                    ('use_low_thres', 'use_high_thres'):[(True, False)],
                    'bbwp_len': [13, 30, 50],
                    'bbwp_lkbk': [64, 128, 256],
                    'low_thres': [2,5,10,20],
                    'high_thres': [100]
                },
                'type': 'env'
            },
            'index2.2': {
                'function': 'index_cond',
                'params': {
                    'function2': ['s_bbwp'],
                    ('use_low_thres', 'use_high_thres'):[(False, True)],
                    'bbwp_len': [13, 30, 50],
                    'bbwp_lkbk': [64, 128, 256],
                    'low_thres': [0],
                    'high_thres': [80, 90, 95, 98]
                },
                'type': 'env'
            },
            'index3': {
                'function': 'index_cond', 
                'params': {
                    'function2': ['s_consecutive_squeezes'],
                    'use_no_sqz': [True]
                },
                'type': 'env'
            },
            'index4': {
                'function': 'index_cond', 
                'params': {
                    'function2': ['s_consecutive_squeezes'],
                    'num_bars': [3, 5, 10, 15, 20]
                },
                'type': 'env'
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
            'index6_env': {
                'function': 'index_cond', 
                'params': {
                    'function2': ['s_ursi'], 
                    'direction': ['above', 'below'],
                    ('use_vs_signal','use_range'): [(True, False), (False,True), (True, True)],
                    ('lower_thres', 'upper_thres'): [(0, 10), (0, 20), (10, 20), (20, 30), (80, 90), (90, 100), (80, 100)]
                },
                'type': 'env'
            },
            'index6_trigger': {
                'function': 'index_cond', 
                'params': {
                    'function2': ['s_ursi'], 
                    'direction': ['crossover', 'crossunder'],
                    ('use_vs_signal','use_range'): [(True, False), (True, True)],
                    ('lower_thres', 'upper_thres'): [(0, 10), (0, 20), (10, 20), (20, 30), (80, 90), (90, 100), (80, 100)]
                },
                'type': 'trigger'
            },
            'lkbk1':{
                'function': 'lookback_cond',
                'params': {
                    'function2': ['price_change'],
                    'periods': [1],
                    'lower_thres': [0, 3, 5, 6.7],
                    'direction': ['increase', 'decrease']
                 },
                 'type': 'trigger'
            },
            'lkbk2':{
                'function': 'lookback_cond',
                
                'params' : {
                    'function2':['price_change'],
                    'periods': [2],
                    'lower_thres': [6, 10, 13],
                    'direction': ['increase', 'decrease']
                },
                'type': 'trigger'
            }, 
            'lkbk3':{
                'function': 'lookback_cond',
                
                'params' : {
                    'function2': ['price_change'],
                    'periods': [3],
                    'lower_thres': [9, 15, 19],
                    'direction': ['increase', 'decrease']
                },
                'type': 'trigger'
            },
            'lkbk4':{
                'function': 'lookback_cond',
                
                'params' : {
                    'function2': ['price_change'],
                    'periods': [5],
                    'lower_thres': [10, 15, 20, 25],
                    'direction': ['increase', 'decrease']
                },
                'type': 'trigger'
            },
            'lkbk5_env':{
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['price_comp_ma'],
                    'ma_len1':[1],
                    'ma_len2':[5,10,15,20,50,200],
                    'ma_dir': ['above', 'below'],
                },
                'type': 'env'
            },
            'lkbk5_trigger':{
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['price_comp_ma'],
                    'ma_len1':[1],
                    'ma_len2':[5,10,15,20,50,200],
                    'ma_dir': ['crossover', 'crossunder'],
                },
                'type': 'trigger'
            },
            'lkbk6_env':{
                'function': 'lookback_cond',
                'params': {
                    'function2': ['price_comp_ma'],
                    'ma_len1':[5],
                    'ma_len2':[15,20],
                    'ma_dir': ['above', 'below'],
                },
                'type':'env'
            },
            'lkbk6_trigger':{
                'function': 'lookback_cond',
                'params': {
                    'function2': ['price_comp_ma'],
                    'ma_len1':[5],
                    'ma_len2':[15,20],
                    'ma_dir': ['crossover', 'crossunder'],
                },
                'type':'trigger'
            },
            'lkbk7_env':{
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['price_comp_ma'],
                    'ma_len1':[10],
                    'ma_len2':[30],
                    'ma_dir': ['above', 'below'],
                },
                'type':'env'
            },
            'lkbk7_trigger':{
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['price_comp_ma'],
                    'ma_len1':[10],
                    'ma_len2':[30],
                    'ma_dir': ['crossover', 'crossunder'],
                },
                'type':'trigger'
            },
            'lkbk8_env':{
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['price_comp_ma'],
                    'ma_len1':[20],
                    'ma_len2':[50, 100],
                    'ma_dir': ['above', 'below'],
                }
            },
            'lkbk8_trigger':{
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['price_comp_ma'],
                    'ma_len1':[20],
                    'ma_len2':[50, 100],
                    'ma_dir': [ 'crossover', 'crossunder'],
                },
                'type':'trigger'
            },
            'lkbk9_env':{
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['price_comp_ma'],
                    'ma_len1':[50],
                    'ma_len2':[200],
                    'ma_dir': ['above', 'below'],
                },
                'type':'env'
            },
            'lkbk9_trigger':{
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['price_comp_ma'],
                    'ma_len1':[50],
                    'ma_len2':[200],
                    'ma_dir': ['crossover', 'crossunder'],
                },
                'type':'trigger'
            },
            'lkbk10':{
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['price_change_vs_hl'],
                    'direction': ['Increase','Decrease'],
                    'nbars': [5, 10, 15, 20, 30],
                    'low_range': [10, 15, 20, 25]
                },
                'type': 'env'
            },
            'lkbk11':{
                'function': 'lookback_cond',
                
                'params':{
                    'function2': ['price_gap'],
                    'gap_dir': ['Use Gap Up', 'Use Gap Down']
                },
                'type':'trigger'
            },
            'lkbk12':{
                'function': 'lookback_cond',
                'params': {
                    'function2': ['price_highest_lowest'],
                    'method': ['Highest', 'Lowest'],
                    'num_bars': [5, 10, 15, 20, 50, 200]
                },
                'type':'trigger'
            },
            'lkbk13':{
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['consecutive_conditional_bars'],
                    ('src1_name', 'src2_name'): [('close', 'close'), ('high', 'low'), ('low', 'low'), ('high', 'high'), ('high', 'close'), ('low', 'close')],
                    'direction': ['Increase', 'Decrease'],
                    ('num_bars', 'num_matched'): [(2,2), (3,3), (4,4), (5,5), (6,6), (7,7), (8,8)]
                },
                'type':'trigger'
            },
            'lkbk14':{
                'function': 'lookback_cond',
                
                'params' : {
                    'function2': ['vol_comp_ma'],
                    ("n_bars", "ma_len") : [(1, 20), (1, 10), (1, 6), (2, 20), (3, 15)],
                    "comp_ma_dir": ["higher", "lower"],
                    "comp_ma_perc": [0, 25, 50, 75, 100, 150]
                },
                'type':'trigger'
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
            'lkbk151_env':{
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['vol_percentile'],
                    'ma_length': [1,3,5,10],
                    'ranking_window': [64, 128, 256],
                    'low_range': [0],
                    'high_range': [10, 15, 20]
                },
                'type':'env'
            },
            'lkbk151_trigger':{
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['vol_percentile'],
                    'ma_length': [1,3,5,10],
                    'ranking_window': [64, 128, 256],
                    'low_range': [0],
                    'high_range': [2,5]
                },
                'type':'trigger'
            },
            'lkbk152_env':{
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['vol_percentile'],
                    'ma_length': [1,3,5,10],
                    'ranking_window': [64, 128, 256],
                    'low_range': [80, 90],
                    'high_range': [100]
                },
                'type':'env'
            },
            'lkbk152_trigger':{
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['vol_percentile'],
                    'ma_length': [1,3,5,10],
                    'ranking_window': [64, 128, 256],
                    'low_range': [95, 98],
                    'high_range': [100]
                },
                'type':'trigger'
            },
            'lkbk16':{
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['consecutive_squeezes'], 
                    'use_no_sqz': [True]
                },
                'type':'env'
            },
            'lkbk17':{
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['consecutive_squeezes'], 
                    'num_bars': [3, 5, 10, 15, 20]
                },
                'type':'env'
            },
            'lkbk18_env' : {
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['bbwp'],
                    ('use_low_thres', 'use_high_thres'):[(True, False)],
                    'bbwp_len': [13, 30, 50],
                    'bbwp_lkbk': [64, 128, 256],
                    'low_thres': [10,20],
                    'high_thres': [100]
                },
                'type':'env'
            },
            'lkbk18_trigger' : {
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['bbwp'],
                    ('use_low_thres', 'use_high_thres'):[(True, False)],
                    'bbwp_len': [13, 30, 50],
                    'bbwp_lkbk': [64, 128, 256],
                    'low_thres': [2,5],
                    'high_thres': [100]
                },
                'type':'trigger'
            },
            'lkbk181_env' : {
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['bbwp'],
                    ('use_low_thres', 'use_high_thres'):[(False, True)],
                    'bbwp_len': [13, 30, 50],
                    'bbwp_lkbk': [64, 128, 256],
                    'low_thres': [0],
                    'high_thres': [80, 90]
                },
                'type':'env'
            },
            'lkbk181_trigger' : {
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['bbwp'],
                    ('use_low_thres', 'use_high_thres'):[(False, True)],
                    'bbwp_len': [13, 30, 50],
                    'bbwp_lkbk': [64, 128, 256],
                    'low_thres': [0],
                    'high_thres': [95, 98]
                },
                'type':'trigger'
            },
            'lkbk19':{
                'function': 'lookback_cond',
                
                'params':{
                    'function2': ['bbpctb'],
                    'length': [20, 50, 100],
                    'mult': [1.5, 2, 2.5, 3],
                    'direction' : ['crossover', 'crossunder'],
                    'cross_line': ['Upper band', 'Lower band']
                },
                'type':'trigger'
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
            'lkbk201_env': {
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['ursi'],
                    'direction': ['above', 'below'],
                    ('use_vs_signal','use_range'): [(True, False), (False,True), (True, True)],
                    ('lower_thres', 'upper_thres'): [(0, 10), (0, 20), (10, 20), (20, 30), (80, 90), (90, 100), (80, 100)]
                },
                'type': 'env'
            },
            'lkbk201_trigger': {
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['ursi'],
                    'direction': ['crossover', 'crossunder'],
                    ('use_vs_signal','use_range'): [(True, False), (True, True)],
                    ('lower_thres', 'upper_thres'): [(0, 10), (0, 20), (10, 20), (20, 30), (80, 90), (90, 100), (80, 100)]
                },
                'type':'trigger'
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
            'lkbk211_env': {
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['macd'],
                    ('use_vs_signal','use_range'): [ (False,True)],
                    'direction': ['crossover', 'crossunder'],
                    ('lower_thres','upper_thres'):[(-9999,0), (0, 9999)]
                },
                'type':'env'
            },
            'lkbk211_trigger': {
                'function': 'lookback_cond',
                
                'params': {
                    'function2': ['macd'],
                    ('use_vs_signal','use_range'): [(True, False),  (True, True)],
                    'direction': ['crossover', 'crossunder'],
                    ('lower_thres','upper_thres'):[(-9999,0), (0, 9999)]
                },
                'type':'trigger'
            },
        }
        
        params_df: pd.DataFrame = None
        for cluster, val in params_dic.items():
            f = val['function']
            p: dict = val['params']
            if 'type' in val:
                t = val['type']
            else:
                t = 'env'
            combinations = list(product(*p.values()))
            dff = pd.DataFrame(combinations, columns=p.keys())
            for c in dff.columns:
                if not isinstance(c, str) :
                    dff[list(c)] = pd.DataFrame(dff[c].tolist(), index=dff.index)
                    dff = dff.drop(c, axis=1)

            dff['function'] = f
            dff['type'] = t
            params_df = pd.concat([params_df, dff], ignore_index=True)

        t = params_df[params_df['function'] == 'index_cond'].dropna(axis=1, how='all')
        return params_df
    
    @staticmethod
    def create_params_sets2():
        params_dic = {
                    'price_change_no_break':{
                        'function': 'price_change_no_break',
                        'params': {
                            'direction': ["Increase", "Decrease"],
                            'nbars': [10,15,20,25,30],
                            'low_range': [10,15,20,25],
                            'high_range': [10000],
                            'use_wait': [True],
                            'wbars': [3,5,7,10],
                            'bound': [2,3,4]
                        },
                        'type': 'trigger'
                    },
                    'percentile1_env':{
                        'function': 'flow_percentile',
                        'params': {
                            'src_name': ['netBUSD', 'netBUSD/Value', 'netForeign', 'netForeign/Value'],
                            'smoothing': [1,5, 10, 15],
                            'lookback': [64, 128, 256],
                            ('position','threshold') : [
                                ('above', 80),
                                ('below', 20),
                                ('above', 90),
                                ('below', 10)
                            ],                        
                        },
                        'type': 'env'
                    },
                    'percentile1_trigger':{
                        'function': 'flow_percentile',
                        'params': {
                            'src_name': ['netBUSD', 'netBUSD/Value', 'netForeign', 'netForeign/Value'],
                            'smoothing': [1,5, 10, 15],
                            'lookback': [64, 128, 256],
                            ('position','threshold') : [
                                ('crossover', 80), 
                                ('crossunder', 20), 
                                ('crossover', 90), 
                                ('crossunder', 10),
                                ('crossover', 95), ('above', 95),
                                ('crossunder', 5), ('below', 5),
                                ('crossover', 98), ('above', 98),
                                ('crossunder', 2), ('below', 2)
                            ],                        
                        },
                        'type': 'trigger'
                    },
                    'consecutive1':{
                        'function': 'flow_consecutive_bars',
                         'params': {
                            'src_name': ['netBUSD', 'netBUSD/Value', 'netForeign', 'netForeign/Value'],
                            'smoothing': [1,5, 10, 15],
                            'direction': ['up', 'down', 'positive','negative'],
                            ('num_conditional_bars', 'num_bars'): [(3,3), (4,4), (5,5), (3,5), (4,5), (3,4)],
                         },
                         'type': 'trigger'
                    },
                    'value1_env':{
                        'function': 'flow_value',
                        'params': {
                            'src_name': ['netBUSD', 'netBUSD/Value', 'netForeign', 'netForeign/Value'],
                            'smoothing': [1,5, 10, 15],
                            'position': ['above', 'below'],
                            'threshold': [0]
                        },
                        'type': 'env'
                    },
                    'value1_trigger':{
                        'function': 'flow_value',
                        'params': {
                            'src_name': ['netBUSD', 'netBUSD/Value', 'netForeign', 'netForeign/Value'],
                            'smoothing': [1,5, 10, 15],
                            'position': ['crossover', 'crossunder'],
                            'threshold': [0]
                        },
                        'type': 'trigger'
                    },
                    'two_mas1_env': {
                        'function': 'flow_two_mas',
                        'params': {
                            'src_name': ['netBUSD', 'netBUSD/Value', 'netForeign', 'netForeign/Value'],
                            ('ma_len1', 'ma_len2'): [(2, 5), (3, 10), (3, 15), (3, 20), (5,10), (5,15), (5,20)],
                            'position': ['above', 'below'],
                        },
                        'type': 'env'
                    },
                    'two_mas_trigger': {
                        'function': 'flow_two_mas',
                        'params': {
                            'src_name': ['netBUSD', 'netBUSD/Value', 'netForeign', 'netForeign/Value'],
                            ('ma_len1', 'ma_len2'): [(2, 5), (3, 10), (3, 15), (3, 20), (5,10), (5,15), (5,20)],
                            'position': ['crossover', 'crossunder'],
                        },
                        'type': 'trigger'
                    },
                    'returns_stock_vs_cluster': {
                        'function': 'returns_stock_vs_cluster_ranked',
                        'params': {
                           'num_bars': [1, 3, 5, 10],
                           'top_or_bottom': ['top', 'bottom'],
                            'ranking': [2,3],
                        },
                        'type': 'env'
                    },
                    'returns_stock_vs_market': {
                        'function': 'returns_stock_vs_index_ranked',
                        'params': {
                            'num_bars': [1, 3, 5, 10],
                            'top_or_bottom': ['top', 'bottom'],
                            'ranking': [2,3],
                        },
                        'type': 'env'
                    },
                    'returns_cluster_vs_market': {
                        'function': 'returns_cluster_vs_index_ranked',
                        'params': {
                            'num_bars': [1, 3, 5, 10],
                            'top_or_bottom': ['top', 'bottom'],
                            'ranking': [2,3],
                        },
                        'type': 'env'
                    },
                    'returns_stock_vs_index_strength1_env': {
                        'function': 'returns_stock_vs_index_strength',
                        'params': {
                            'num_return_bars': [1, 3, 5, 10],
                            'ma_use_flag': [True],
                            ('ma_len1', 'ma_len2'): [(1,10), (5, 15), (10, 30), (20, 50), (20, 100)],
                            'ma_position': [ 'above', 'below'],
                        },
                        'type': 'env'
                    },
                    'returns_stock_vs_index_strength1_trigger': {
                        'function': 'returns_stock_vs_index_strength',
                        'params': {
                            'num_return_bars': [1, 3, 5, 10],
                            'ma_use_flag': [True],
                            ('ma_len1', 'ma_len2'): [(1,10), (5, 15), (10, 30), (20, 50), (20, 100)],
                            'ma_position': ['crossover', 'crossunder'],
                        },
                        'type': 'trigger'
                    },
                    'returns_stock_vs_index_strength2': {
                        'function': 'returns_stock_vs_index_strength',
                        'params': {
                            'num_return_bars': [1, 3, 5, 10],
                            'bb_use_flag': [True],
                            'bb_length': [10,15,20,25],
                            'bb_mult' : [1, 1.5, 2, 2.5],
                            'bb_position': ['crossover', 'crossunder'],
                            'bb_cross_line': ['upper', 'lower'],
                        },
                        'type': 'trigger'
                    },
                    'returns_stock_vs_index_strength3_env': {
                        'function' : 'returns_stock_vs_index_strength',
                        'params': {
                            'num_return_bars': [1, 3, 5, 10],
                            'pct_use_flag': [True],
                            'pct_lookback': [64, 128, 256],
                            ('pct_hline_position','pct_hline') : [
                                ('above', 80),
                                ('below', 20),
                                ('above', 90),
                                ('below', 10),
                            ],    
                        },
                        'type': 'env'
                    },
                    'returns_stock_vs_index_strength3_trigger': {
                        'function' : 'returns_stock_vs_index_strength',
                        'params': {
                            'num_return_bars': [1, 3, 5, 10],
                            'pct_use_flag': [True],
                            'pct_lookback': [64, 128, 256],
                            ('pct_hline_position','pct_hline') : [
                                ('crossover', 80), 
                                ('crossunder', 20), 
                                ('crossover', 90), 
                                ('crossunder', 10),
                                ('crossover', 95), ('above', 95),
                                ('crossunder', 5), ('below', 5),
                                ('crossover', 98), ('above', 98),
                                ('crossunder', 2), ('below', 2)
                            ],    
                        },
                        'type': 'trigger'
                    },
                    'returns_stock_vs_cluster_strength1_env': {
                        'function': 'returns_stock_vs_cluster_strength',
                        'params': {
                            'num_return_bars': [1, 3, 5, 10],
                            'ma_use_flag': [True],
                            ('ma_len1', 'ma_len2'): [(1,10), (5, 15), (10, 30), (20, 50), (20, 100)],
                            'ma_position': [ 'above', 'below'],
                        },
                        'type': 'env'
                    },
                    'returns_stock_vs_cluster_strength1_trigger': {
                        'function': 'returns_stock_vs_cluster_strength',
                        'params': {
                            'num_return_bars': [1, 3, 5, 10],
                            'ma_use_flag': [True],
                            ('ma_len1', 'ma_len2'): [(1,10), (5, 15), (10, 30), (20, 50), (20, 100)],
                            'ma_position': ['crossover', 'crossunder'],
                        },
                        'type': 'trigger'
                    },
                    'returns_stock_vs_cluster_strength2': {
                        'function': 'returns_stock_vs_cluster_strength',
                        'params': {
                            'num_return_bars': [1, 3, 5, 10],
                            'bb_use_flag': [True],
                            'bb_length': [10,15,20,25],
                            'bb_mult' : [1, 1.5, 2, 2.5],
                            'bb_position': ['crossover', 'crossunder'],
                            'bb_cross_line': ['upper', 'lower'],
                        },
                        'type': 'trigger'
                    },
                    'returns_stock_vs_cluster_strength3_env': {
                        'function' : 'returns_stock_vs_cluster_strength',
                        'params': {
                            'num_return_bars': [1, 3, 5, 10],
                            'pct_use_flag': [True],
                            'pct_lookback': [64, 128, 256],
                            ('pct_hline_position','pct_hline') : [
                                ('above', 80),
                                ('below', 20),
                                ('above', 90),
                                ('below', 10),
                            ],    
                        },
                        'type': 'env'
                    },
                    'returns_stock_vs_cluster_strength3_trigger': {
                        'function' : 'returns_stock_vs_cluster_strength',
                        'params': {
                            'num_return_bars': [1, 3, 5, 10],
                            'pct_use_flag': [True],
                            'pct_lookback': [64, 128, 256],
                            ('pct_hline_position','pct_hline') : [
                                ('crossover', 80), 
                                ('crossunder', 20), 
                                ('crossover', 90), 
                                ('crossunder', 10), 
                                ('crossover', 95), ('above', 95),
                                ('crossunder', 5), ('below', 5),
                                ('crossover', 98), ('above', 98),
                                ('crossunder', 2), ('below', 2)
                            ],    
                        },
                        'type': 'trigger'
                    },
                    'returns_cluster_vs_index_strength1_env': {
                        'function': 'returns_cluster_vs_index_strength',
                        'params': {
                            'num_return_bars': [1, 3, 5, 10],
                            'ma_use_flag': [True],
                            ('ma_len1', 'ma_len2'): [(1,10), (5, 15), (10, 30), (20, 50), (20, 100)],
                            'ma_position': [ 'above', 'below'],
                        },
                        'type': 'env'
                    },
                    'returns_cluster_vs_index_strength1_trigger': {
                        'function': 'returns_cluster_vs_index_strength',
                        'params': {
                            'num_return_bars': [1, 3, 5, 10],
                            'ma_use_flag': [True],
                            ('ma_len1', 'ma_len2'): [(1,10), (5, 15), (10, 30), (20, 50), (20, 100)],
                            'ma_position': ['crossover', 'crossunder'],
                        },
                        'type': 'trigger'
                    },
                    'returns_cluster_vs_index_strength2': {
                        'function': 'returns_cluster_vs_index_strength',
                        'params': {
                            'num_return_bars': [1, 3, 5, 10],
                            'bb_use_flag': [True],
                            'bb_length': [10,15,20,25],
                            'bb_mult' : [1, 1.5, 2, 2.5],
                            'bb_position': ['crossover', 'crossunder'],
                            'bb_cross_line': ['upper', 'lower'],
                        },
                        'type': 'trigger'
                    },
                    'returns_cluster_vs_index_strength3_env': {
                        'function' : 'returns_cluster_vs_index_strength',
                        'params': {
                            'num_return_bars': [1, 3, 5, 10],
                            'pct_use_flag': [True],
                            'pct_lookback': [64, 128, 256],
                            ('pct_hline_position','pct_hline') : [
                                ('above', 80),
                                ('below', 20),
                                ('above', 90),
                                ('below', 10),
                            ],    
                        },
                        'type': 'env'
                    },
                    'returns_cluster_vs_index_strength3_trigger': {
                        'function' : 'returns_cluster_vs_index_strength',
                        'params': {
                            'num_return_bars': [1, 3, 5, 10],
                            'pct_use_flag': [True],
                            'pct_lookback': [64, 128, 256],
                            ('pct_hline_position','pct_hline') : [
                                ('crossover', 80), 
                                ('crossunder', 20),
                                ('crossover', 90),
                                ('crossunder', 10), 
                                ('crossover', 95), ('above', 95),
                                ('crossunder', 5), ('below', 5),
                                ('crossover', 98), ('above', 98),
                                ('crossunder', 2), ('below', 2)
                            ],    
                        },
                        'type': 'trigger'
                    },
                    'consecutive_performance' : {
                        'function': 'consecutive_performance',
                        'params': {
                            'num_return_bars': [1, 3, 5, 10],
                            'direction': ['positive', 'negative'],
                            ('num_conditional_bars','num_consecutive_bars'): [(1,1), (3,3), (5,5), (10,10), (4,5), (8,10), (9,10)],
                        },
                        'type': 'trigger'
                    },
                    'volume_stock_vs_index1_env': {
                        'function': 'volume_stock_vs_index',
                        'params': {
                            'smoothing': [1, 5, 10, 15],
                            'ma_use_flag': [True],
                            ('ma_len1', 'ma_len2'): [(1,10), (5, 15), (10, 30), (20, 50), (20, 100)],
                            'ma_position': ['above', 'below'],
                        },
                        'type': 'env'
                    },
                    'volume_stock_vs_index1_trigger': {
                        'function': 'volume_stock_vs_index',
                        'params': {
                            'smoothing': [1, 5, 10, 15],
                            'ma_use_flag': [True],
                            ('ma_len1', 'ma_len2'): [(1,10), (5, 15), (10, 30), (20, 50), (20, 100)],
                            'ma_position': ['crossover', 'crossunder'],
                        },
                        'type': 'trigger'
                    },
                    'volume_stock_vs_index2': {
                        'function': 'volume_stock_vs_index',
                        'params': {
                            'smoothing': [1, 5, 10, 15],
                            'bb_use_flag': [True],
                            'bb_length': [10,15,20,25],
                            'bb_mult' : [1, 1.5, 2, 2.5],
                            'bb_position': ['crossover', 'crossunder'],
                            'bb_cross_line': ['upper', 'lower'],
                        },
                        'type': 'trigger'
                    },
                    'volume_stock_vs_index3_env': {
                        'function' : 'volume_stock_vs_index',
                        'params': {
                            'smoothing': [1, 5, 10, 15],
                            'pct_use_flag': [True],
                            'pct_lookback': [64, 128, 256],
                            ('pct_hline_position','pct_hline') : [
                                ('above', 80),
                                ('below', 20),
                                ('above', 90),
                                ('below', 10)
                            ],    
                        },
                        'type': 'env'
                    },
                    'volume_stock_vs_index3_trigger': {
                        'function' : 'volume_stock_vs_index',
                        'params': {
                            'smoothing': [1, 5, 10, 15],
                            'pct_use_flag': [True],
                            'pct_lookback': [64, 128, 256],
                            ('pct_hline_position','pct_hline') : [
                                ('crossover', 80), 
                                ('crossunder', 20), 
                                ('crossover', 90),
                                ('crossunder', 10), 
                                ('crossover', 95), ('above', 95),
                                ('crossunder', 5), ('below', 5),
                                ('crossover', 98), ('above', 98),
                                ('crossunder', 2), ('below', 2)
                            ],    
                        },
                        'type': 'trigger'
                    },
                    'volume_stock_vs_cluster1_env': {
                        'function': 'volume_stock_vs_cluster',
                        'params': {
                            'smoothing': [1, 5, 10, 15],
                            'ma_use_flag': [True],
                            ('ma_len1', 'ma_len2'): [(1,10), (5, 15), (10, 30), (20, 50), (20, 100)],
                            'ma_position': [ 'above', 'below'],
                        },
                        'type' : 'env'
                    },
                    'volume_stock_vs_cluster1_trigger': {
                        'function': 'volume_stock_vs_cluster',
                        'params': {
                            'smoothing': [1, 5, 10, 15],
                            'ma_use_flag': [True],
                            ('ma_len1', 'ma_len2'): [(1,10), (5, 15), (10, 30), (20, 50), (20, 100)],
                            'ma_position': ['crossover', 'crossunder'],
                        },
                        'type' : 'trigger'
                    },
                    'volume_stock_vs_cluster2': {
                        'function': 'volume_stock_vs_cluster',
                        'params': {
                            'smoothing': [1, 5, 10, 15],
                            'bb_use_flag': [True],
                            'bb_length': [10,15,20,25],
                            'bb_mult' : [1, 1.5, 2, 2.5],
                            'bb_position': ['crossover', 'crossunder'],
                            'bb_cross_line': ['upper', 'lower'],
                        },
                        'type':'trigger'
                    },
                    'volume_stock_vs_cluster3_env': {
                        'function' : 'volume_stock_vs_cluster',
                        'params': {
                            'smoothing': [1, 5, 10, 15],
                            'pct_use_flag': [True],
                            'pct_lookback': [64, 128, 256],
                            ('pct_hline_position','pct_hline') : [
                                ('above', 80),
                                ('below', 20),
                                ('above', 90),
                                ('below', 10)
                          
                            ],    
                        },
                        'type': 'env'
                    },
                    'volume_stock_vs_cluster3_trigger': {
                        'function' : 'volume_stock_vs_cluster',
                        'params': {
                            'smoothing': [1, 5, 10, 15],
                            'pct_use_flag': [True],
                            'pct_lookback': [64, 128, 256],
                            ('pct_hline_position','pct_hline') : [
                                ('crossover', 80), 
                                ('crossunder', 20),
                                ('crossover', 90), 
                                ('crossunder', 10), 
                                ('crossover', 95), ('above', 95),
                                ('crossunder', 5), ('below', 5),
                                ('crossover', 98), ('above', 98),
                                ('crossunder', 2), ('below', 2)
                            ],    
                        },
                        'type': 'trigger'
                    },
                     'volume_cluster_vs_index1_env': {
                        'function': 'volume_cluster_vs_index',
                        'params': {
                            'smoothing': [1, 5, 10, 15],
                            'ma_use_flag': [True],
                            ('ma_len1', 'ma_len2'): [(1,10), (5, 15), (10, 30), (20, 50), (20, 100)],
                            'ma_position': [ 'above', 'below'],
                        },
                        'type': 'env'
                    },
                    'volume_cluster_vs_index1_trigger': {
                        'function': 'volume_cluster_vs_index',
                        'params': {
                            'smoothing': [1, 5, 10, 15],
                            'ma_use_flag': [True],
                            ('ma_len1', 'ma_len2'): [(1,10), (5, 15), (10, 30), (20, 50), (20, 100)],
                            'ma_position': ['crossover', 'crossunder'],
                        },
                        'type': 'trigger'
                    },
                    'volume_cluster_vs_index2': {
                        'function': 'volume_cluster_vs_index',
                        'params': {
                            'smoothing': [1, 5, 10, 15],
                            'bb_use_flag': [True],
                            'bb_length': [10,15,20,25],
                            'bb_mult' : [1, 1.5, 2, 2.5],
                            'bb_position': ['crossover', 'crossunder'],
                            'bb_cross_line': ['upper', 'lower'],
                        },
                        'type':'trigger'
                    },
                    'volume_cluster_vs_index3_env': {
                        'function' : 'volume_cluster_vs_index',
                        'params': {
                            'smoothing': [1, 5, 10, 15],
                            'pct_use_flag': [True],
                            'pct_lookback': [64, 128, 256],
                            ('pct_hline_position','pct_hline') : [
                                ('above', 80),
                                ('below', 20),
                                ('above', 90),
                                ('below', 10)
                             
                            ],    
                        },
                        'type': 'env'
                    },
                    'volume_cluster_vs_index3_trigger': {
                        'function' : 'volume_cluster_vs_index',
                        'params': {
                            'smoothing': [1, 5, 10, 15],
                            'pct_use_flag': [True],
                            'pct_lookback': [64, 128, 256],
                            ('pct_hline_position','pct_hline') : [
                                ('crossover', 80), 
                                ('crossunder', 20), 
                                ('crossover', 90), 
                                ('crossunder', 10), 
                                ('crossover', 95), ('above', 95),
                                ('crossunder', 5), ('below', 5),
                                ('crossover', 98), ('above', 98),
                                ('crossunder', 2), ('below', 2)
                            ],    
                        },
                        'type': 'trigger'
                    },
                    'volume_cluster_vs_cluster_consecutive1': {
                        'function' : 'volume_cluster_vs_cluster_consecutive',
                        'params': {
                            'smoothing': [1, 5, 10, 15],
                            'num_bars': [5, 10],
                            'top_or_bottom': ['top', 'bottom'],
                            'ranking': [2,3],
                            ('num_conditional_bars', 'consecutive_bars'): [(3,3), (4,4), (5,5), (3,5), (4,5), (3,4), (8,10), (9,10), (10,10)],
                        },
                        'type': 'trigger'
        }
        }


        params_df: pd.DataFrame = None
        for cluster, val in params_dic.items():
            f = val['function']
            p: dict = val['params']
            if 'type' in val:
                t = val['type']
            else:
                t = 'env'
            combinations = list(product(*p.values()))
            dff = pd.DataFrame(combinations, columns=p.keys())
            for c in dff.columns:
                if not isinstance(c, str) :
                    dff[list(c)] = pd.DataFrame(dff[c].tolist(), index=dff.index)
                    dff = dff.drop(c, axis=1)

            dff['function'] = f
            dff['type'] = t
            params_df = pd.concat([params_df, dff], ignore_index=True)
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
                    for patt in ['len', 'bar', 'lkbk', 'period', 'ranking']:
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
    def calc_and_push_data_to_plasma(
        push_stocks_numpy: bool = False, 
        push_return_numpy: bool = False,
        push_ursi_numpy: bool = False,
        push_ru_dd_numpy: bool = False,
        push_uptrend_downtrend: bool = False,
        push_label_rudd: bool = False,
        push_label_triple: bool = False,
        push_peak_trough: bool = False,
        shift: bool = False
    ):
        _, client_disconnect, psave, pload = gen_plasma_functions(db=5)
        df = glob_obj.df_stocks
        close: pd.DataFrame = df['close']
        stocks_map = {k: v for k, v in enumerate(close.columns)}
        day_ls = close.index.to_list()
        day_ls = [i for i in day_ls if i >= '2018_01_01']

        if push_stocks_numpy:
            psave("df_stocks", df)

        if push_return_numpy:
            df_return: pd.DataFrame  = (df['open'].shift(-16) / df['open'].shift(-1) - 1) * 100

            df_return = get_from_day(df_return, '2018_01_01')
            return_num = df_return.to_numpy()

            psave("return_array", return_num)

        if push_ursi_numpy:
            src = df['close']
            df_ursi, _ = Ta.ursi(src)

            df_ursi = get_from_day(df_ursi, '2018_01_01')
            ursi_num = df_ursi.to_numpy()

            psave("ursi_vectorized", ursi_num)

        if push_ru_dd_numpy:
            df_max_high = df['high'].rolling(15).max().shift(-15)
            df_close_price = df['open'].shift(-16)
            df_runup = df_max_high.where(df_max_high > df_close_price, df_close_price)
            df_runup = (df_runup / df['open'].shift(-1) - 1) * 100
            df_runup = get_from_day(df_runup, '2018_01_01')
            ru_num = df_runup.to_numpy()
            psave("runup_vectorized", ru_num)

            df_min_low = df['low'].rolling(15).min().shift(-15)
            df_close_price = df['open'].shift(-16)
            df_drawdown = df_min_low.where(df_min_low < df_close_price, df_close_price)
            df_drawdown = (df_drawdown / df['open'].shift(-1) - 1) * 100
            df_drawdown = get_from_day(df_drawdown, '2018_01_01')
            dd_num = df_drawdown.to_numpy()
            psave("drawdown_vectorized", dd_num)

        if push_label_rudd:
            df_label_rudd = pd.read_pickle('/home/ubuntu/Tai/daily_indicators/pickle/df_label_stats.pickle')
            df_label_rudd = get_from_day(df_label_rudd, '2018_01_01')
            label_rudd_num = df_label_rudd.to_numpy()
            psave("label_rudd", label_rudd_num)

        if push_uptrend_downtrend:
            start_day = '2018_01_01'
            df_index = glob_obj.df_vnindex
            df_index = df_index[df_index['day'].isin(df['day'])].reset_index(drop=True)
            exception_days = [d for d in df['day'].to_list() if d not in df_index['day'].to_list()]

            MA1 = 1
            MA2 = 20
            MA3 = 50


            df_ut = Conds.price_comp_ma(df_index, ma_len1= MA1, ma_len2= MA2, ma_dir='above') & Conds.price_comp_ma(df_index, ma_len1= MA2, ma_len2= MA3, ma_dir='above')
            df_ld = Conds.price_comp_ma(df_index, ma_len1= MA3, ma_len2= MA2, ma_dir='above') & Conds.price_comp_ma(df_index, ma_len1= MA2, ma_len2= MA1, ma_dir='above')
            df_pb = Conds.price_comp_ma(df_index, ma_len1= MA2, ma_len2= MA1, ma_dir='above') & Conds.price_comp_ma(df_index, ma_len1= MA1, ma_len2= MA3, ma_dir='above')
            df_rc = Conds.price_comp_ma(df_index, ma_len1= MA1, ma_len2= MA2, ma_dir='above') & Conds.price_comp_ma(df_index, ma_len1= MA3, ma_len2= MA2, ma_dir='above')
            df_ed = Conds.price_comp_ma(df_index, ma_len1= MA2, ma_len2= MA3, ma_dir='above') & Conds.price_comp_ma(df_index, ma_len1= MA3, ma_len2= MA1, ma_dir='above')

            df_ut.index = df_index['day']
            df_ld.index = df_index['day']
            df_pb.index = df_index['day']
            df_rc.index = df_index['day']
            df_ed.index = df_index['day']

            for d in exception_days:
                df_ut.loc[d] = False
                df_ld.loc[d] = False
                df_pb.loc[d] = False
                df_rc.loc[d] = False
                df_ed.loc[d] = False

            df_ut = df_ut.sort_index()
            df_ld = df_ld.sort_index()
            df_pb = df_pb.sort_index()
            df_rc = df_rc.sort_index()
            df_ed = df_ed.sort_index()

            if shift: 
                df_ut = df_ut.shift(5)
                df_ld = df_ld.shift(5)
                df_pb = df_pb.shift(5)
                df_rc = df_rc.shift(5)
                df_ed = df_ed.shift(5)

            ut_arr = df_ut[df_ut.index >= start_day].to_numpy()
            ld_arr = df_ld[df_ld.index >= start_day].to_numpy()
            pb_arr = df_pb[df_pb.index >= start_day].to_numpy()
            rc_arr = df_rc[df_rc.index >= start_day].to_numpy()
            ed_arr = df_ed[df_ed.index >= start_day].to_numpy()


            psave("ut_array", ut_arr)
            psave("ld_array", ld_arr)
            psave("pb_array", pb_arr)
            psave("rc_array", rc_arr)
            psave("ed_array", ed_arr)

        # if push_peak_trough:
        #     start_day = '2018_01_01'
        #     df_index = glob_obj.df_vnindex
        #     df_index = df_index[df_index['day'].isin(df['day'])].reset_index(drop=True)
        #     exception_days = [d for d in df['day'].to_list() if d not in df_index['day'].to_list()]

            leftBar_trough = 31
            rightBar_trough = 21
            leftThreshold_trough = 7
            rightThreshold_trough = 4

            bars_to_keep = 5

            df_index['right_upside'] = ((df_index['high'].rolling(rightBar_trough).max() / df_index['low'].rolling(rightBar_trough).min()).shift(-rightBar_trough)  - 1) * 100
            df_index['left_downside'] = ((df_index['low'].rolling(leftBar_trough).min() / df_index['high'].rolling(leftBar_trough).max())  - 1) * 100

            df_index['pivotLow'] = (df_index['low'] == df_index['low'].rolling(leftBar_trough).min()) & \
                                   (df_index['low'] == df_index['low'].rolling(rightBar_trough+1).min().shift(-rightBar_trough))

            df_index['trough'] = df_index['pivotLow'] & \
                                 (df_index['left_downside'].abs() >= leftThreshold_trough) & \
                                 (df_index['right_upside'].abs() >= rightThreshold_trough)

            df_index['trough_count'] = df_index['trough'].cumsum() 
            df_index['consecutive_after_trough'] = (df_index['trough'] == False).groupby(df_index['trough_count']).cumcount()
            df_index['consecutive_before_trough'] = (df_index['trough'] == False)[::-1].groupby((df_index['trough'] == True)[::-1].cumsum()).cumcount()[::-1]

            df_index['after_trough'] = df_index['consecutive_after_trough'] < bars_to_keep
            df_index['before_trough'] = df_index['consecutive_before_trough'] < bars_to_keep
            df_index.loc[df_index.index[-bars_to_keep:] ,'before_trough'] = False

            ###
            leftBar_peak = 31
            rightBar_peak = 16
            leftThreshold_peak = 2
            rightThreshold_peak = 7

            df_index['right_downside'] = ((df_index['low'].rolling(rightBar_peak).min() / df_index['high'].rolling(rightBar_peak).max()).shift(-rightBar_peak)  - 1) * 100
            df_index['left_upside'] = ((df_index['high'].rolling(leftBar_peak).max() / df_index['low'].rolling(leftBar_peak).min())  - 1) * 100

            df_index['pivotHigh'] = (df_index['high'] == df_index['high'].rolling(leftBar_peak).max()) & \
                                   (df_index['high'] == df_index['high'].rolling(rightBar_peak+1).max().shift(-rightBar_peak))

            df_index['peak'] = df_index['pivotHigh'] & \
                                 (df_index['left_upside'].abs() >= leftThreshold_peak) & \
                                 (df_index['right_upside'].abs() >= rightThreshold_peak)
            
            df_index['peak_count'] = df_index['peak'].cumsum() 
            df_index['consecutive_after_peak'] = (df_index['peak'] == False).groupby(df_index['peak_count']).cumcount()
            df_index['consecutive_before_peak'] = (df_index['peak'] == False)[::-1].groupby((df_index['peak'] == True)[::-1].cumsum()).cumcount()[::-1]

            df_index['after_peak'] = df_index['consecutive_after_peak'] < bars_to_keep
            df_index['before_peak'] = df_index['consecutive_before_peak'] < bars_to_keep
            df_index.loc[df_index.index[-bars_to_keep:] ,'before_peak'] = False

            
            df_index = df_index[df_index['day']>=start_day]
            day_index = glob_obj.df_stocks.index.to_frame().reset_index(drop=True)
            day_index = day_index[day_index['day']>=start_day]
            df_index = df_index.merge(day_index, on ='day', how ='right')
            for d in exception_days:
                df_index.loc[df_index['day']==d,'before_trough'] = False
                df_index.loc[df_index['day']==d,'after_trough'] = False
                df_index.loc[df_index['day']==d,'before_peak'] = False
                df_index.loc[df_index['day']==d,'after_peak'] = False
            df_index = df_index.sort_values('day')

            before_trough_arr = df_index['before_trough'].to_numpy()
            after_trough_arr = df_index['after_trough'].to_numpy()

            before_peak_arr = df_index['before_peak'].to_numpy()
            after_peak_arr = df_index['after_peak'].to_numpy()

            psave("before_trough_arr", before_trough_arr)
            psave("after_trough_arr", after_trough_arr)
            psave("before_peak_arr", before_peak_arr)
            psave("after_peak_arr", after_peak_arr)

        client_disconnect()

        return stocks_map, day_ls
    
    class MultiProcess:
        @staticmethod
        def compute_signals_general(recompute = True, params_df: pd.DataFrame = None, plasma_name: str = "sig_3d_array_general"):
            from danglib.pylabview2.celery_worker import compute_signal, clean_redis
            
            n_strats = len(params_df)

            if recompute:
                _, client_disconnect, psave, pload = gen_plasma_functions(db=5)
                df_tmp = df['close']
                df_tmp = df_tmp[df_tmp.index >= '2018_01_01']
                n_rows, n_cols = df_tmp.shape

                # Khởi tạo một mảng NumPy 3 chiều với kích thước (2000, 2000, 200)
                array_3d = np.empty((n_strats, n_rows, n_cols))

                task_dic = {}
                print("Computing signals:")
                for idx, params in tqdm(params_df.iterrows(), total=params_df.shape[0]):    
                    params = params.dropna().to_dict()
                    params.pop('type',None)
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
                psave(plasma_name,array_3d)

                clean_redis()
                client_disconnect()

            return n_strats
        @staticmethod
        def compute_signals_env(recompute = True):
            from danglib.pylabview2.celery_worker import compute_signal, clean_redis

            params_df_old: pd.DataFrame = Vectorized.create_params_sets()
            params_df_new: pd.DataFrame = Vectorized.create_params_sets2()
            params_df = pd.concat([params_df_old, params_df_new], ignore_index=True)
            params_df = params_df[params_df['type'] == 'env'].reset_index(drop=True)
            
            n_strats = len(params_df)

            if recompute:
                _, client_disconnect, psave, pload = gen_plasma_functions(db=5)
                df_tmp = df['close']
                df_tmp = df_tmp[df_tmp.index >= '2018_01_01']
                n_rows, n_cols = df_tmp.shape

                # Khởi tạo một mảng NumPy 3 chiều với kích thước (2000, 2000, 200)
                array_3d = np.empty((n_strats, n_rows, n_cols))

                task_dic = {}
                print("Computing signals:")
                for idx, params in tqdm(params_df.iterrows(), total=params_df.shape[0]):    
                    params = params.dropna().to_dict()
                    params.pop('type',None)
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
                psave("sig_3d_array_env",array_3d)

                clean_redis()
                client_disconnect()

            return n_strats
        @staticmethod
        def compute_signals_trigger(recompute = True):
            from danglib.pylabview2.celery_worker import compute_signal, clean_redis

            params_df_old: pd.DataFrame = Vectorized.create_params_sets()
            params_df_new: pd.DataFrame = Vectorized.create_params_sets2()
            params_df = pd.concat([params_df_old, params_df_new], ignore_index=True)
            params_df = params_df[params_df['type'] == 'trigger'].reset_index(drop=True)
            n_strats = len(params_df)

            if recompute:
                _, client_disconnect, psave, pload = gen_plasma_functions(db=5)
                df_tmp = df['close']
                df_tmp = df_tmp[df_tmp.index >= '2018_01_01']
                n_rows, n_cols = df_tmp.shape

                # Khởi tạo một mảng NumPy 3 chiều với kích thước (2000, 2000, 200)
                array_3d = np.empty((n_strats, n_rows, n_cols))

                task_dic = {}
                print("Computing signals:")
                for idx, params in tqdm(params_df.iterrows(), total=params_df.shape[0]):    
                    params = params.dropna().to_dict()
                    params.pop('type',None)
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
                psave("sig_3d_array_trigger",array_3d)

                clean_redis()
                client_disconnect()

            return n_strats
        @staticmethod
        def compute_signals2(recompute = True):
            from danglib.pylabview2.celery_worker import compute_signal2, clean_redis

            # params_dic = pd.read_pickle('/home/ubuntu/Tai/classify/dic_original_params.pickle')
            params_dic = pd.read_pickle('/home/ubuntu/Tai/classify/environment/dic_indexed_params.pickle')
            n_strats = len(params_dic)

            if recompute:
                _, client_disconnect, psave, pload = gen_plasma_functions(db=5)
                df_tmp = df['close']
                df_tmp = df_tmp[(df_tmp.index >= '2018_01_01') & (df_tmp.index <= '2025_01_03')]
                n_rows, n_cols = df_tmp.shape

                # Khởi tạo một mảng NumPy 3 chiều với kích thước (2000, 2000, 200)
                array_3d = np.empty((n_strats, n_rows, n_cols))

                task_dic = {}
                print("Computing signals:")
                for idx, params in tqdm(enumerate(params_dic.values()), total=n_strats):    
                    task_dic[idx] = compute_signal2.delay(idx, params)

                while any(t.status!='SUCCESS' for t in task_dic.values()):
                    pass
                
                for idx, v in task_dic.items():
                    res: pd.DataFrame = v.result
                    if res is not None:
                        array_3d[idx, :, :] = res

                    else:
                        print(f"{idx} error")
                array_3d = array_3d.astype(bool)
                psave("sig_array2",array_3d)

                clean_redis()
                client_disconnect()

            return n_strats
        
        @staticmethod
        def compute_signals_cfm(recompute = True):
            from danglib.pylabview2.celery_worker import compute_signal2, clean_redis

            # params_dic = pd.read_pickle('/home/ubuntu/Tai/classify/dic_cfm_params.pickle')
            params_dic = pd.read_pickle('/home/ubuntu/Tai/classify/environment/dic_cfm_params.pickle')
            n_strats = len(params_dic)

            if recompute:
                _, client_disconnect, psave, pload = gen_plasma_functions(db=5)
                df_tmp = df['close']
                df_tmp = df_tmp[(df_tmp.index >= '2018_01_01') & (df_tmp.index <= '2025_01_03')]
                n_rows, n_cols = df_tmp.shape

                # Khởi tạo một mảng NumPy 3 chiều với kích thước (2000, 2000, 200)
                array_3d = np.empty((n_strats, n_rows, n_cols))

                task_dic = {}
                print("Computing signals:")
                for idx, params in tqdm(enumerate(params_dic.values()), total=n_strats):    
                    task_dic[idx] = compute_signal2.delay(idx, params)

                while any(t.status!='SUCCESS' for t in task_dic.values()):
                    pass
                
                for idx, v in task_dic.items():
                    res: pd.DataFrame = v.result
                    if res is not None:
                        array_3d[idx, :, :] = res

                    else:
                        print(f"{idx} error")
                array_3d = array_3d.astype(bool)
                psave("sig_array_cfm",array_3d)

                clean_redis()
                client_disconnect()

            return n_strats
        @staticmethod
        def compute_wr_re_nt_for_all_strats_cfm(n_solo_strats, folder):
            from danglib.pylabview2.celery_worker import clean_redis, compute_multi_strategies_cfm
            clean_redis()
            task_dic = {}
            print("Computing stats")
            for i in tqdm(range(0, n_solo_strats)):
                task_dic[i] = compute_multi_strategies_cfm.delay(i, folder)

            while any(t.status!='SUCCESS' for t in task_dic.values()):
                pass

            clean_redis()
        @staticmethod
        def compute_wr_re_nt_new_combination2_daylist(n_solo_strats, lst_idx, folder):
            from danglib.pylabview2.celery_worker import clean_redis, compute_multi_strategies_new_combi2_daylist
            clean_redis()
            task_dic = {}
            print("Computing stats")
            for i in tqdm(range(0, n_solo_strats)):
                task_dic[i] = compute_multi_strategies_new_combi2_daylist.delay(i, lst_idx, folder)

            while any(t.status!='SUCCESS' for t in task_dic.values()):
                pass

            clean_redis()
        @staticmethod
        def compute_wr_re_nt_new_combination2(n_solo_strats, folder):
            from danglib.pylabview2.celery_worker import clean_redis, compute_multi_strategies_new_combi2
            clean_redis()
            task_dic = {}
            print("Computing stats")
            for i in tqdm(range(0, n_solo_strats)):
                task_dic[i] = compute_multi_strategies_new_combi2.delay(i, folder)

            while any(t.status!='SUCCESS' for t in task_dic.values()):
                pass

            clean_redis()
        @staticmethod
        def compute_wr_re_nt_new_combination(n_solo_strats, folder):
            from danglib.pylabview2.celery_worker import clean_redis, compute_multi_strategies_new_combi
            clean_redis()
            task_dic = {}
            print("Computing stats")
            for i in tqdm(range(0, n_solo_strats)):
                task_dic[i] = compute_multi_strategies_new_combi.delay(i, folder)

            while any(t.status!='SUCCESS' for t in task_dic.values()):
                pass

            clean_redis()
        @staticmethod
        def compute_wr_re_nt_new_combination_os(n_solo_strats, folder):
            from danglib.pylabview2.celery_worker import clean_redis, compute_multi_strategies_new_combi_os
            clean_redis()
            task_dic = {}
            print("Computing stats")
            for i in tqdm(range(0, n_solo_strats)):
                task_dic[i] = compute_multi_strategies_new_combi_os.delay(i, folder)

            while any(t.status!='SUCCESS' for t in task_dic.values()):
                pass

            clean_redis()
        @staticmethod
        def compute_wr_re_new_cond(n_solo_strats, folder):
            from danglib.pylabview2.celery_worker import clean_redis, compute_multi_strategies_new_cond
            clean_redis()
            task_dic = {}
            print("Computing stats")
            for i in tqdm(range(0, n_solo_strats -1)):
                task_dic[i] = compute_multi_strategies_new_cond.delay(i, folder)

            while any(t.status!='SUCCESS' for t in task_dic.values()):
                pass

            clean_redis()
        @staticmethod
        def compute_wr_re_nt_for_all_strats_os(n_solo_strats, folder):
            from danglib.pylabview2.celery_worker import clean_redis, compute_multi_strategies_os
            clean_redis()
            task_dic = {}
            print("Computing stats")
            for i in tqdm(range(0, n_solo_strats -1)):
                task_dic[i] = compute_multi_strategies_os.delay(i, folder)

            while any(t.status!='SUCCESS' for t in task_dic.values()):
                pass

            clean_redis()
        @staticmethod
        def compute_num_days_for_all_strats_cfm(n_solo_strats, folder):
            from danglib.pylabview2.celery_worker import clean_redis, compute_multi_strategies_num_days
            clean_redis()
            task_dic = {}
            print("Computing stats")
            for i in tqdm(range(0, n_solo_strats)):
                task_dic[i] = compute_multi_strategies_num_days.delay(i, folder)

            while any(t.status!='SUCCESS' for t in task_dic.values()):
                pass

            clean_redis()

        @staticmethod
        def compute_ursi_for_all_strats_cfm(n_solo_strats, folder):
            from danglib.pylabview2.celery_worker import clean_redis, compute_multi_strategies_ursi_cfm
            clean_redis()
            task_dic = {}
            print("Computing stats")
            for i in tqdm(range(0, n_solo_strats)):
                task_dic[i] = compute_multi_strategies_ursi_cfm.delay(i, folder)

            while any(t.status!='SUCCESS' for t in task_dic.values()):
                pass

            clean_redis()


        @staticmethod
        def compute_signals_new_cond(recompute = True):
            from danglib.pylabview2.celery_worker import compute_signal, clean_redis

            params_df_old_cond: pd.DataFrame = Vectorized.create_params_sets()
            params_df: pd.DataFrame = Vectorized.create_params_sets2()
            params_df = pd.concat([params_df_old_cond, params_df], ignore_index=True)
            n_strats = len(params_df)

            if recompute:
                _, client_disconnect, psave, pload = gen_plasma_functions(db=5)
                df_tmp = df['close']
                df_tmp = df_tmp[df_tmp.index >= '2018_01_01']
                n_rows, n_cols = df_tmp.shape

                # Khởi tạo một mảng NumPy 3 chiều với kích thước (2000, 2000, 200)
                array_3d = np.empty((n_strats, n_rows, n_cols))

                task_dic = {}
                print("Computing signals:")
                for idx, params in tqdm(params_df.iterrows(), total=params_df.shape[0]):    
                    params = params.dropna().to_dict()
                    params.pop('type', None)
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
                psave("sig_3d_array_new_cond",array_3d)

                clean_redis()
                client_disconnect()

            return n_strats



        @staticmethod
        def compute_signals(recompute = True):
            from danglib.pylabview2.celery_worker import compute_signal, clean_redis

            params_df: pd.DataFrame = Vectorized.create_params_sets()
            n_strats = len(params_df)

            if recompute:
                _, client_disconnect, psave, pload = gen_plasma_functions(db=5)
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
                client_disconnect()

            return n_strats

        @staticmethod
        def compute_wr_re_nt_for_all_strats(n_solo_strats, folder):
            from danglib.pylabview2.celery_worker import clean_redis, compute_multi_strategies
            clean_redis()
            task_dic = {}
            print("Computing stats")
            for i in tqdm(range(0, n_solo_strats-1)):
                task_dic[i] = compute_multi_strategies.delay(i, folder)

            while any(t.status!='SUCCESS' for t in task_dic.values()):
                pass

            clean_redis()
        @staticmethod
        def compute_new_label_for_all_strats(n_solo_strats, folder):
            from danglib.pylabview2.celery_worker import clean_redis, compute_multi_strategies_label
            clean_redis()
            task_dic = {}
            print("Computing stats")
            for i in tqdm(range(0, n_solo_strats-1)):
                task_dic[i] = compute_multi_strategies_label.delay(i, folder)

            while any(t.status!='SUCCESS' for t in task_dic.values()):
                pass

            clean_redis()
        @staticmethod
        def compute_wr_re_nt_2(n_strats, start_date_idx, end_date_idx, folder):
            from danglib.pylabview2.celery_worker import clean_redis, compute_multi_strategies_2
            clean_redis()
            task_dic = {}
            for i in tqdm(range(0, n_strats-1)):
                task_dic[i] = compute_multi_strategies_2.delay(i, start_date_idx, end_date_idx, folder)

            while any(t.status!='SUCCESS' for t in task_dic.values()):
                pass

            clean_redis()


        @staticmethod
        def compute_ursi_for_all_strats(n_solo_strats, folder):
            from danglib.pylabview2.celery_worker import clean_redis, compute_multi_strategies_ursi
            clean_redis()
            
            maybe_create_dir(f"{folder}/stocks_count_day")
            maybe_create_dir(f"{folder}/ursi_sum")

            task_dic = {}
            print("Computing stats")
            for i in tqdm(range(0, n_solo_strats-1)):
                task_dic[i] = compute_multi_strategies_ursi.delay(i, folder)

            while any(t.status!='SUCCESS' for t in task_dic.values()):
                pass

            clean_redis()

        @staticmethod
        def compute_runup_drawdown_for_all_strats(n_solo_strats, folder):
            from danglib.pylabview2.celery_worker import clean_redis, compute_multi_strategies_ru_dd
            clean_redis()

            task_dic = {}
            print("Computing stats")
            for i in tqdm(range(0, n_solo_strats-1)):
                task_dic[i] = compute_multi_strategies_ru_dd.delay(i, folder=folder)

            while any(t.status!='SUCCESS' for t in task_dic.values()):
                pass

            clean_redis()

        @staticmethod
        def compute_sharpe_for_all_strats(n_solo_strats, folder):
            from danglib.pylabview2.celery_worker import clean_redis, compute_multi_strategies_sharpe
            clean_redis()

            task_dic = {}
            print("Computing stats")
            for i in tqdm(range(0, n_solo_strats-1)):
                task_dic[i] = compute_multi_strategies_sharpe.delay(i, folder=folder)

            while any(t.status!='SUCCESS' for t in task_dic.values()):
                pass

            clean_redis()

        @staticmethod
        def compute_ut_dt_for_all_strats(n_solo_strats, folder):
            from danglib.pylabview2.celery_worker import clean_redis, compute_multi_strategies_utdt
            clean_redis()
            task_dic = {}
            print("Computing stats")
            for i in tqdm(range(0, n_solo_strats-1)):
                task_dic[i] = compute_multi_strategies_utdt.delay(i, folder)

            while any(t.status!='SUCCESS' for t in task_dic.values()):
                pass

            clean_redis()


    class JoinResults:
        @staticmethod
        def join_wr_re_nt_data_os(n, stocks_map, src_folder, des_folder):
            fns = walk_through_files(src_folder)

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
                dir = f"{des_folder}/df_{name}.pkl"
                write_pickle(dir, res)
                print(dir)

            join_one_stats('nt')
            join_one_stats('wt')
            join_one_stats('re')
        # @staticmethod
        # def join_wr_re_nt_data_cfm(stocks_map, src_folder, des_folder):
        #     fns = walk_through_files(src_folder)

        #     def join_one_stats(name):

        #         # df_res: pd.DataFrame = None
        #         # res = []
        #         res = pd.DataFrame()
        #         print(f"Join {name} stats: ")
        #         for f in tqdm(fns):
        #             f: str
        #             i = int(f.split('/')[-1].split(".")[0].split("_")[1])
        #             if i < 10000:
        #                 nt_raw, re_raw, wt_raw = pd.read_pickle(f)
        #                 src = None
        #                 if name == 'nt':
        #                     src = nt_raw
        #                     del re_raw
        #                     del wt_raw
        #                 if name == 're':
        #                     src = re_raw
        #                 if name == 'wt':
        #                     src = wt_raw

        #                 tmp = pd.DataFrame(src)
        #                 tmp[-1] = i 
        #                 # res.append(tmp)
        #                 res = pd.concat([res, tmp], axis=0)
        #                 del tmp
                    
        #         # res: pd.DataFrame = pd.concat(res)
                
        #         res = res.reset_index(names=-2)
        #         cols_map = stocks_map.copy()
        #         cols_map[-1] = 'i'
        #         cols_map[-2] = 'j'

        #         res = res.rename(columns = cols_map)
        #         res = res.set_index(['i', 'j'])
        #         dir = f"{des_folder}/df_{name}.pkl"
        #         write_pickle(dir, res)
        #         # import pyarrow 
        #         # res.to_parquet(f"{dir}", engine='pyarrow')

        #         print(dir)

        #     join_one_stats('nt')
        #     # join_one_stats('wt')
        #     # join_one_stats('re')
        @staticmethod
        def join_wr_re_nt_data_cfm(stocks_map, src_folder, des_folder, name = 'nt'):
   

            lsf = FileHandler.walk_through_files(src_folder)
            ls_nt = []
            # for i in tqdm(range(0, 100)):
            for i in tqdm(range(0, len(lsf))):
                idx = int(lsf[i].split('/')[-1].split(".")[0].split("_")[1])

                if name == 'nt':
                    nt, _, _ = FileHandler.read_pickle(lsf[i])
                    new_column = np.full((nt.shape[0], 1), idx)
                    nt = np.column_stack((nt, new_column))
                    row_indices = np.arange(nt.shape[0]).reshape(-1, 1)
                    nt = np.column_stack((nt, row_indices))
                    nt = nt.astype(np.int32)
                    ls_nt.append(nt)
                    del nt, _, new_column
                elif name == 're':
                    _, re, _ = FileHandler.read_pickle(lsf[i])
                    new_column = np.full((re.shape[0], 1), idx)
                    re = np.column_stack((re, new_column))
                    row_indices = np.arange(re.shape[0]).reshape(-1, 1)
                    re = np.column_stack((re, row_indices))
                    re = re.astype(np.float32)
                    ls_nt.append(re)
                    del re, _, new_column
                elif name == 'wt':
                    _, _, wt = FileHandler.read_pickle(lsf[i])
                    new_column = np.full((wt.shape[0], 1), idx)
                    wt = np.column_stack((wt, new_column))
                    row_indices = np.arange(wt.shape[0]).reshape(-1, 1)
                    wt = np.column_stack((wt, row_indices))
                    wt = wt.astype(np.int32)
                    ls_nt.append(wt)
                    del wt, _, new_column

            big_arr = np.concatenate(ls_nt, axis=0)
            res = pd.DataFrame(big_arr)
            res = res.rename(columns = {195:-1, 196:-2})
            # res = res.reset_index(names=-2)
            cols_map = stocks_map.copy()
            cols_map[-1] = 'i'
            cols_map[-2] = 'j'

            res = res.rename(columns = cols_map)
            res = res.set_index(['i', 'j'])

            dir = f"{des_folder}/df_{name}.pkl"
            write_pickle(dir, res)
            print(dir)

        @staticmethod
        def join_wr_re_nt_data_new_combination(stocks_map, src_folder, des_folder):
            fns = walk_through_files(src_folder)

            def join_one_stats(name):

                # df_res: pd.DataFrame = None
                res = []
                print(f"Join {name} stats: ")
                for f in tqdm(fns):
                    f: str
                    i = int(f.split('/')[-1].split(".")[0].split("_")[1])
                    if i < 10000:
                        nt_raw, re_raw, wt_raw = pd.read_pickle(f)
                        src = None
                        if name == 'nt':
                            src = nt_raw
                        if name == 're':
                            src = re_raw
                        if name == 'wt':
                            src = wt_raw

                        tmp = pd.DataFrame(src)
                        tmp[-1] = i 
                        res.append(tmp)
                    
                res: pd.DataFrame = pd.concat(res)
                res = res.reset_index(names=-2)
                cols_map = stocks_map.copy()
                cols_map[-1] = 'i'
                cols_map[-2] = 'j'

                res = res.rename(columns = cols_map)
                res = res.set_index(['i', 'j'])
                dir = f"{des_folder}/df_{name}.pkl"
                write_pickle(dir, res)
                print(dir)

            join_one_stats('nt')
            join_one_stats('wt')
            join_one_stats('re')

        @staticmethod
        def join_num_days_data_cfm(stocks_map, src_folder, des_folder):
            fns = walk_through_files(src_folder)

            def join_one_stats(name):
                name = 'num_days'
                # df_res: pd.DataFrame = None
                res = []
                print(f"Join {name} stats: ")
                for f in tqdm(fns):
                    f: str
                    i = int(f.split('/')[-1].split(".")[0].split("_")[1])
                    if i < 2013:
                        nd_raw = pd.read_pickle(f)
                    

                        tmp = pd.DataFrame(nd_raw)
                        tmp[-1] = i 
                        res.append(tmp)
                    
                res: pd.DataFrame = pd.concat(res)
                res = res.reset_index(names=-2)
                cols_map = {0:'num_days'}
                cols_map[-1] = 'i'
                cols_map[-2] = 'j'

                res = res.rename(columns = cols_map)
                res = res.set_index(['i', 'j'])
                dir = f"{des_folder}/df_{name}.pkl"
                write_pickle(dir, res)
                print(dir)

            join_one_stats('nt')

        @staticmethod
        def join_ursi_data_cfm(src_folder, des_folder):
            fns2 = walk_through_files(src_folder)
            print(f"Join ursi: ")
            # df_res: pd.DataFrame = None
            res2 = []

            for f in tqdm(fns2):
                f: str
                i = int(f.split('/')[-1].split(".")[0].split("_")[1])

                src = pd.read_pickle(f)
                src['i'] = i 
                res2.append(src)
            res2: pd.DataFrame = pd.concat(res2)
            res2 = res2.set_index(['i', 'j'])
            write_pickle(f"{des_folder}/strats_stocks_ursi.pkl", res2)

        @staticmethod
        def join_new_label_data(n, stocks_map, src_folder, des_folder):
            fns = walk_through_files(src_folder)

            def join_one_stats(name):

                # df_res: pd.DataFrame = None
                res = []
                print(f"Join {name} stats: ")
                for f in tqdm(fns):
                    f: str
                    i = int(f.split('/')[-1].split(".")[0].split("_")[1])
                    if i < 2013:
                        nt_raw, gd_raw, bd_raw, nd_raw = pd.read_pickle(f)
                        src = None
                        if name == 'nt':
                            src = nt_raw
                        if name == 'gd':
                            src = gd_raw
                        if name == 'bd':
                            src = bd_raw
                        if name == 'nd':
                            src = nd_raw

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
                dir = f"{des_folder}/df_{name}.pkl"
                write_pickle(dir, res)
                print(dir)

            join_one_stats('nt')
            join_one_stats('gd')
            join_one_stats('bd')
            join_one_stats('nd')

        @staticmethod
        def join_wr_re_nt_data(n, stocks_map, src_folder, des_folder):
            fns = walk_through_files(src_folder)

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
                dir = f"{des_folder}/df_{name}.pkl"
                write_pickle(dir, res)
                print(dir)

            join_one_stats('nt')
            join_one_stats('wt')
            join_one_stats('re')



        @staticmethod
        def join_wr_re_nt_data_2(n, stocks_map, src_folder, result_folder):
            def test():
                n = 1418
                stocks_map = stocks_map
                dir = '/data/dang/tmp2/nt_wr_re_0_250'
                file_name = '2018'


            fns = walk_through_files(src_folder)

            def join_one_stats(name):
                res = []
                print(f"Join {name} stats: ")
                for f in tqdm(fns):
                    f: str
                    i = int(f.split('/')[-1].split(".")[0].split("_")[1])
                    if i < 2013:
                        nt_raw, re_raw, wt_raw = pd.read_pickle(f)
                        src = None
                        if name == f'nt':
                            src = nt_raw
                        if name == f're':
                            src = re_raw
                        if name == f'wt':
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

                fn = f"{result_folder}/df_{name}.pkl"
                print(f'Write data to {fn}')
                write_pickle(fn, res)

            join_one_stats(f'nt')
            join_one_stats(f'wt')
            join_one_stats(f're')


        @staticmethod
        def join_ru_dd_data(n, src_folder, des_folder):
            def test():
                n = 1418
                stocks_map = stocks_map
                dir = '/data/dang/tmp2/ru_dd_0_-1'
                file_name = '2018'


            fns = walk_through_files(src_folder)

            def join_one_stats(name):

                # df_res: pd.DataFrame = None
                res = []
                print(f"Join {name} stats: ")
                for f in tqdm(fns):
                    f: str
                    i = int(f.split('/')[-1].split(".")[0].split("_")[1])
                    if i < 2013:
                        ru_raw, dd_raw = pd.read_pickle(f)
                        src = None
                        if name == f'ru':
                            src = ru_raw
                            cols_map = {0:'avg_ru'}

                        if name == f'dd':
                            src = dd_raw
                            cols_map = {0:'avg_dd'}

                        tmp = pd.DataFrame(src, index=list(range(i+1, n)))
                        tmp[-1] = i 
                        res.append(tmp)
                res: pd.DataFrame = pd.concat(res)
                res = res.reset_index(names=-2)
                # cols_map = stocks_map.copy()
                cols_map[-1] = 'i'
                cols_map[-2] = 'j'

                res = res.rename(columns = cols_map)
                res = res.set_index(['i', 'j'])
                write_pickle(f"{des_folder}/df_{name}.pkl", res)

            join_one_stats(f'ru')
            join_one_stats(f'dd')
            # join_one_stats(f'{file_name}_re')

            
        @staticmethod
        def join_sharpe_files(n, stocks_map, src_folder, des_folder):
            def test():
                n =  1418
                
            fns = walk_through_files(src_folder)
            print(f"Join sharpe temp files: ")
            # df_res: pd.DataFrame = None
            res = []

            for f in tqdm(fns):
                f: str
                i = int(f.split('/')[-1].split(".")[0].split("_")[1])
                if i < 2013:
                    src = pd.read_pickle(f)
                        
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
            write_pickle(f"{des_folder}/df_sharpe.pkl", res)


        @staticmethod
        def join_ursi_result(n, day_ls, src_folder, des_folder):
            def test():
                n =  1418
                
            fns = walk_through_files(f"{src_folder}/stocks_count_day")
            print(f"Join stocks count: ")
            # df_res: pd.DataFrame = None
            res = []

            for f in tqdm(fns):
                f: str
                i = int(f.split('/')[-1].split(".")[0].split("_")[1])

                src = pd.read_pickle(f)
                tmp = pd.DataFrame(src, index=list(range(i+1, n)))
                tmp[-1] = i 
                res.append(tmp)
            res: pd.DataFrame = pd.concat(res)
            res = res.reset_index(names=-2)
            cols = [int(i) for i in day_ls]
            res = res.rename(columns={-1:'i', -2:'j'})

            # res = res.rename(columns = day_ls)
            res = res.set_index(['i', 'j'])
            res.columns = cols
            res.sort_index(axis=1)
            
            write_pickle(f"{des_folder}/stocks_matched_day.pkl", res)


            fns2 = walk_through_files(f"{src_folder}/ursi_sum")
            print(f"Join ursi: ")
            # df_res: pd.DataFrame = None
            res2 = []

            for f in tqdm(fns2):
                f: str
                i = int(f.split('/')[-1].split(".")[0].split("_")[1])

                src = pd.read_pickle(f)
                src['i'] = i 
                res2.append(src)
            res2: pd.DataFrame = pd.concat(res2)
            res2 = res2.set_index(['i', 'j'])

            write_pickle(f"{des_folder}/strats_stocks_ursi.pkl", res2)

        @staticmethod
        def join_ut_dt_data(n, stocks_map, src_folder, des_folder):
            fns = walk_through_files(src_folder)

            def join_one_stats(name):

                # df_res: pd.DataFrame = None
                res = []
                print(f"Join {name} stats: ")
                for f in tqdm(fns):
                    f: str
                    i = int(f.split('/')[-1].split(".")[0].split("_")[1])
                    if i < 2013:
                        ut_raw, ld_raw, pb_raw, rc_raw, ed_raw, bt_raw, at_raw, bp_raw, ap_raw = pd.read_pickle(f)
                        src = None
                        if name == 'uptrend':
                            src = ut_raw
                        if name == 'downtrend':
                            src = ld_raw
                        if name == 'pullback':
                            src = pb_raw
                        if name == 'recovery':
                            src = rc_raw
                        if name == 'early_downtrend':
                            src = ed_raw

                        if name == 'before_trough':
                            src = bt_raw
                        if name == 'after_trough':
                            src = at_raw
                        if name == 'before_peak':
                            src = bp_raw
                        if name == 'after_peak':
                            src = ap_raw

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
                dir = f"{des_folder}/df_{name}.pkl"
                write_pickle(dir, res)
                print(dir)

            join_one_stats('uptrend')
            join_one_stats('downtrend')
            join_one_stats('pullback')
            join_one_stats('recovery')
            join_one_stats('early_downtrend')

            join_one_stats('before_trough')
            join_one_stats('after_trough')
            join_one_stats('before_peak')
            join_one_stats('after_peak')


    class Runs:
        @staticmethod
        def compute_nt_re_wr_new_combination2_daylist():
            # name = 'new_combination4_yearly'
            name = 'new_combination2_regime_duy'
            store_folder = f"/data/Tai/{name}_tmp"
            maybe_create_dir(store_folder)

            result_folder = f"/data/Tai/pickles/{name}"
            maybe_create_dir(result_folder)
            
            recompute_signals = True
            ## CALC  -------------------------------------------------------------------
            
            stocks_map, day_ls = Vectorized.calc_and_push_data_to_plasma(
                                                push_return_numpy=True, 
                                                push_stocks_numpy=True
                                            )
            params_df_old: pd.DataFrame = Vectorized.create_params_sets()
            params_df_new: pd.DataFrame = Vectorized.create_params_sets2()
            params_df = pd.concat([params_df_old, params_df_new], ignore_index=True)
            params_df_env = params_df[params_df['type'] == 'env'].reset_index(drop=True)
            params_df_trigger = params_df[params_df['type'] == 'trigger'].reset_index(drop=True)

            import pickle
            # with open('/home/ubuntu/Tai/classify/new_indicators/chosen_env.pickle', 'rb') as f:
            #     dic_chosen_env = pickle.load(f)
            # with open('/home/ubuntu/Tai/classify/new_indicators/chosen_trigger.pickle', 'rb') as f:
            #     dic_chosen_trigger = pickle.load(f)
            # chosen_params_df_env = params_df_env[params_df_env.index.isin(dic_chosen_env.values())].reset_index(drop=True)

            with open('/home/ubuntu/Tai/classify/new_indicators/lst_chosen_env.pickle', 'rb') as f:
                lst_chosen_env = pickle.load(f)
            with open('/home/ubuntu/Tai/classify/new_indicators/lst_chosen_trigger.pickle', 'rb') as f:
                lst_chosen_trigger = pickle.load(f)
            chosen_params_df_env = params_df_env[params_df_env.index.isin(lst_chosen_env)].reset_index(drop=True)
            chosen_params_df_trigger = params_df_trigger[params_df_trigger.index.isin(lst_chosen_trigger)].reset_index(drop=True)
            n_strats_env = Vectorized.MultiProcess.compute_signals_general(recompute_signals, params_df =chosen_params_df_env, plasma_name = 'sig_3d_array_env_chosen') 
            n_strats_trigger = Vectorized.MultiProcess.compute_signals_general(recompute_signals, params_df = chosen_params_df_trigger, plasma_name = 'sig_3d_array_trigger_chosen')

            _, disconnect, psave, pload = gen_plasma_functions(db=5)
            array_3d = pload("sig_3d_array_env_chosen")
            array_3d = array_3d[:,:1665, :]

            # Store all pairwise AND operations
            from itertools import combinations
            pairwise_and_results = []
            pair_indices = []

            for i, j in combinations(range(array_3d.shape[0]), 2):
                result = np.logical_and(array_3d[i], array_3d[j])
                pairwise_and_results.append(result)
                pair_indices.append((i, j))
                
            with open('/home/ubuntu/Tai/classify/new_indicators/pair_indices.pickle', 'wb') as f:
                pickle.dump(pair_indices, f)
            pairwise_and_array = np.array(pairwise_and_results)
            psave("sig_3d_array_2envs",pairwise_and_array)

            year_map = {}
            year = None
            for idx, d in enumerate(day_ls):
                curr_year = int(d[0:4])
                if curr_year != year:
                    year_map[curr_year] = [idx]
                    if idx != 0:
                        year_map[year].append(idx)
                    year = curr_year
            year_map[year].append(idx+1)
            

            for year, idxs in year_map.items():
                start_idx, end_idx = idxs
                lst_idx = list(range(start_idx, end_idx))
                sub_store_folder = f"{store_folder}/{year}"
                maybe_create_dir(sub_store_folder)

                sub_res_folder = f"{result_folder}/{year}"
                maybe_create_dir(sub_res_folder)

                print(f"Computing stats for {year} ... ", end = ' ')
                Vectorized.MultiProcess.compute_wr_re_nt_new_combination2_daylist(pairwise_and_array.shape[0], lst_idx, folder=sub_store_folder)
                print(f"Joining files ... ", end = ' ')
                print(f"Finished!")
            print("---------------------------------------------------------")
            print('Done!')
            del year_map[2025]
            for year, idxs in year_map.items():
                start_idx, end_idx = idxs
                lst_idx = list(range(start_idx, end_idx))
                sub_store_folder = f"{store_folder}/{year}"
                maybe_create_dir(sub_store_folder)

                sub_res_folder = f"{result_folder}/{year}"
                maybe_create_dir(sub_res_folder)

                # print(f"Computing stats for {year} ... ", end = ' ')
                # Vectorized.MultiProcess.compute_wr_re_nt_new_combination2_daylist(pairwise_and_array.shape[0], lst_idx, folder=sub_store_folder)
                print(f"Joining files ... ", end = ' ')
                Vectorized.JoinResults.join_wr_re_nt_data_cfm(stocks_map, src_folder=sub_store_folder, des_folder=sub_res_folder, name ='nt')
                Vectorized.JoinResults.join_wr_re_nt_data_cfm(stocks_map, src_folder=sub_store_folder, des_folder=sub_res_folder, name ='re')
                Vectorized.JoinResults.join_wr_re_nt_data_cfm(stocks_map, src_folder=sub_store_folder, des_folder=sub_res_folder, name ='wt')
                print(f"Finished!")
            print("---------------------------------------------------------")
            print('Done!')

            df_regime_duy = pd.read_pickle('/home/ubuntu/Tai/classify/new_indicators/resources/df_regime_label.pkl')
            df_regime_duy = df_regime_duy.reset_index(drop=True)
            # for regime in df_regime_duy['regime_label'].unique():
            for regime in ['ACCUMULATION', 'UNKNOWN_SIDEWAYS', 'DISTRIBUTION']:
                sub_store_folder = f"{store_folder}/{regime}"
                maybe_create_dir(sub_store_folder)

                sub_res_folder = f"{result_folder}/{regime}"
                maybe_create_dir(sub_res_folder)
                lst_idx = df_regime_duy[df_regime_duy['regime_label'] == regime].index.tolist()
                print(f"Computing stats for {regime} ... ", end = ' ')
                Vectorized.MultiProcess.compute_wr_re_nt_new_combination2_daylist(pairwise_and_array.shape[0], lst_idx, folder=sub_store_folder)


        @staticmethod
        def compute_nt_re_wr_new_combination2():
            name = 'new_combination4'
            store_folder = f"/data/Tai/{name}_tmp"
            maybe_create_dir(store_folder)

            result_folder = f"/data/Tai/pickles/{name}"
            maybe_create_dir(result_folder)
            
            recompute_signals = True
            ## CALC  -------------------------------------------------------------------
            
            stocks_map, day_ls = Vectorized.calc_and_push_data_to_plasma(
                                                push_return_numpy=True, 
                                                push_stocks_numpy=True
                                            )
            params_df_old: pd.DataFrame = Vectorized.create_params_sets()
            params_df_new: pd.DataFrame = Vectorized.create_params_sets2()
            params_df = pd.concat([params_df_old, params_df_new], ignore_index=True)
            params_df_env = params_df[params_df['type'] == 'env'].reset_index(drop=True)
            params_df_trigger = params_df[params_df['type'] == 'trigger'].reset_index(drop=True)

            import pickle
            # with open('/home/ubuntu/Tai/classify/new_indicators/chosen_env.pickle', 'rb') as f:
            #     dic_chosen_env = pickle.load(f)
            # with open('/home/ubuntu/Tai/classify/new_indicators/chosen_trigger.pickle', 'rb') as f:
            #     dic_chosen_trigger = pickle.load(f)
            # chosen_params_df_env = params_df_env[params_df_env.index.isin(dic_chosen_env.values())].reset_index(drop=True)

            with open('/home/ubuntu/Tai/classify/new_indicators/lst_chosen_env.pickle', 'rb') as f:
                lst_chosen_env = pickle.load(f)
            with open('/home/ubuntu/Tai/classify/new_indicators/lst_chosen_trigger.pickle', 'rb') as f:
                lst_chosen_trigger = pickle.load(f)
            chosen_params_df_env = params_df_env[params_df_env.index.isin(lst_chosen_env)].reset_index(drop=True)
            chosen_params_df_trigger = params_df_trigger[params_df_trigger.index.isin(lst_chosen_trigger)].reset_index(drop=True)
            n_strats_env = Vectorized.MultiProcess.compute_signals_general(recompute_signals, params_df =chosen_params_df_env, plasma_name = 'sig_3d_array_env_chosen') 
            n_strats_trigger = Vectorized.MultiProcess.compute_signals_general(recompute_signals, params_df = chosen_params_df_trigger, plasma_name = 'sig_3d_array_trigger_chosen')

            _, disconnect, psave, pload = gen_plasma_functions(db=5)
            array_3d = pload("sig_3d_array_env_chosen")
            array_3d = array_3d[:,:1665, :]

            # Store all pairwise AND operations
            from itertools import combinations
            pairwise_and_results = []
            pair_indices = []

            for i, j in combinations(range(array_3d.shape[0]), 2):
                result = np.logical_and(array_3d[i], array_3d[j])
                pairwise_and_results.append(result)
                pair_indices.append((i, j))
                
            with open('/home/ubuntu/Tai/classify/new_indicators/pair_indices.pickle', 'wb') as f:
                pickle.dump(pair_indices, f)
            pairwise_and_array = np.array(pairwise_and_results)
            psave("sig_3d_array_2envs",pairwise_and_array)

            Vectorized.MultiProcess.compute_wr_re_nt_new_combination2(pairwise_and_array.shape[0], folder=store_folder)
            Vectorized.JoinResults.join_wr_re_nt_data_cfm(stocks_map, src_folder=store_folder, des_folder=result_folder, name ='nt')
            Vectorized.JoinResults.join_wr_re_nt_data_cfm(stocks_map, src_folder=store_folder, des_folder=result_folder, name ='re')
            Vectorized.JoinResults.join_wr_re_nt_data_cfm(stocks_map, src_folder=store_folder, des_folder=result_folder, name ='wt')

        @staticmethod
        def compute_nt_re_wr_new_combination():
            name = 'new_combination2'
            store_folder = f"/data/Tai/{name}_tmp"
            maybe_create_dir(store_folder)

            result_folder = f"/data/Tai/pickles/{name}"
            maybe_create_dir(result_folder)
            
            recompute_signals = True
            ## CALC  -------------------------------------------------------------------
            
            stocks_map, day_ls = Vectorized.calc_and_push_data_to_plasma(
                                                push_return_numpy=True, 
                                                push_stocks_numpy=True
                                            )
            
            n_strats_env = Vectorized.MultiProcess.compute_signals_env(recompute_signals)
            n_strats_trigger = Vectorized.MultiProcess.compute_signals_trigger(recompute_signals)

            Vectorized.MultiProcess.compute_wr_re_nt_new_combination(n_strats_env, folder=store_folder)
            Vectorized.JoinResults.join_wr_re_nt_data_cfm(stocks_map, src_folder=store_folder, des_folder=result_folder, name = 'nt')
        @staticmethod
        def compute_nt_re_wr_new_combination_os():
            name = 'new_combination_os'
            store_folder = f"/data/Tai/{name}"
            maybe_create_dir(store_folder)

            result_folder = f"/data/Tai/pickles/{name}"
            maybe_create_dir(result_folder)
            
            recompute_signals = True
            ## CALC  -------------------------------------------------------------------
            
            stocks_map, day_ls = Vectorized.calc_and_push_data_to_plasma(
                                                push_return_numpy=True, 
                                                push_stocks_numpy=True
                                            )
            
            n_strats_env = Vectorized.MultiProcess.compute_signals_env(recompute_signals)
            n_strats_trigger = Vectorized.MultiProcess.compute_signals_trigger(recompute_signals)

            Vectorized.MultiProcess.compute_wr_re_nt_new_combination_os(n_strats_env, folder=store_folder)
            Vectorized.JoinResults.join_wr_re_nt_data_cfm(stocks_map, src_folder=store_folder, des_folder=result_folder)
        @staticmethod
        def compute_nt_re_wr_new_cond2():
            name = 'new_cond_test'
            store_folder = f"/data/Tai/{name}_tmp"
            maybe_create_dir(store_folder)

            result_folder = f"/data/Tai/pickles/{name}"
            maybe_create_dir(result_folder)
            
            # params_df = Vectorized.create_params_sets2()
            recompute_signals = True
            ## CALC  -------------------------------------------------------------------
            
            stocks_map, day_ls = Vectorized.calc_and_push_data_to_plasma(
                                                push_return_numpy=True, 
                                                push_stocks_numpy=True,
                                                push_label_rudd= True
                                            )
            
            n_strats = Vectorized.MultiProcess.compute_signals_new_cond(recompute_signals)
            _, disconnect, psave, pload = gen_plasma_functions(db=5)

            array_3d = pload("sig_3d_array_new_cond")
            array_3d_os = array_3d[:,1665:1803, :]
            array_3d = array_3d[:,:1665, :]

            re_2d = pload("return_array")
            re_2d_os = re_2d[1665:1803]
            re_2d = re_2d[:1665]

            label_rudd_2d = pload("label_rudd")
            label_rudd_2d_os = label_rudd_2d[1665:1803]
            label_rudd_2d = label_rudd_2d[:1665]

            num_trade = np.sum(array_3d, axis=1)
            arr_return_3d = array_3d * re_2d
            arr_return_3d = np.nan_to_num(arr_return_3d, 0.0)
            arr_total_return = np.sum(arr_return_3d, axis=1)
            arr_num_win = np.sum(arr_return_3d > 0, axis=1)

            df_nt = pd.DataFrame(num_trade, columns = stocks_map.values())
            df_re = pd.DataFrame(arr_total_return, columns = stocks_map.values())
            df_wt = pd.DataFrame(arr_num_win, columns = stocks_map.values())
            
            df_nt.to_pickle(f"{result_folder}/df_nt.pkl")
            df_re.to_pickle(f"{result_folder}/df_re.pkl")
            df_wt.to_pickle(f"{result_folder}/df_wt.pkl")

            # label
            good_days = (array_3d * (label_rudd_2d==1)).sum(axis =1)
            bad_days = (array_3d * (label_rudd_2d==-1)).sum(axis =1)
            neutral_days = (array_3d * (label_rudd_2d==0)).sum(axis =1)

            df_gd = pd.DataFrame(good_days, columns = stocks_map.values())
            df_bd = pd.DataFrame(bad_days, columns = stocks_map.values())
            df_nd = pd.DataFrame(neutral_days, columns = stocks_map.values())

            df_gd.to_pickle(f"{result_folder}/df_gd.pkl")
            df_bd.to_pickle(f"{result_folder}/df_bd.pkl")
            df_nd.to_pickle(f"{result_folder}/df_nd.pkl")

            ### os 
            def calculate_stats(array_3d, re_2d, label_rudd_2d, stocks_map):
                num_trade = np.sum(array_3d, axis=1)
                arr_return_3d = array_3d * re_2d
                arr_return_3d = np.nan_to_num(arr_return_3d, 0.0)
                arr_total_return = np.sum(arr_return_3d, axis=1)
                arr_num_win = np.sum(arr_return_3d > 0, axis=1)

                df_nt = pd.DataFrame(num_trade, columns = stocks_map.values())
                df_re = pd.DataFrame(arr_total_return, columns = stocks_map.values())
                df_wt = pd.DataFrame(arr_num_win, columns = stocks_map.values())
                
                good_days = (array_3d * (label_rudd_2d==1)).sum(axis =1)
                bad_days = (array_3d * (label_rudd_2d==-1)).sum(axis =1)
                neutral_days = (array_3d * (label_rudd_2d==0)).sum(axis =1)

                df_gd = pd.DataFrame(good_days, columns = stocks_map.values())
                df_bd = pd.DataFrame(bad_days, columns = stocks_map.values())
                df_nd = pd.DataFrame(neutral_days, columns = stocks_map.values())

                return df_nt, df_re, df_wt, df_gd, df_bd, df_nd
            df_nt_os, df_re_os, df_wt_os, df_gd_os, df_bd_os, df_nd_os = calculate_stats(array_3d_os, re_2d_os, label_rudd_2d_os, stocks_map)
            df_nt_os.to_pickle(f"{result_folder}/df_nt_os.pkl")
            df_re_os.to_pickle(f"{result_folder}/df_re_os.pkl")
            df_wt_os.to_pickle(f"{result_folder}/df_wt_os.pkl")
            df_gd_os.to_pickle(f"{result_folder}/df_gd_os.pkl")
            df_bd_os.to_pickle(f"{result_folder}/df_bd_os.pkl")
            df_nd_os.to_pickle(f"{result_folder}/df_nd_os.pkl")

        @staticmethod
        def compute_nt_re_wr_os():
            ## PARAMS-------------------------------------------------------------------
            # name = 'nt_re_wr_3'
            name = 'os_3'
            store_folder = f"/data/Tai/{name}_tmp"
            maybe_create_dir(store_folder)

            result_folder = f"/data/Tai/pickles/{name}"
            maybe_create_dir(result_folder)

            recompute_signals = True
            ## CALC  -------------------------------------------------------------------
            
            stocks_map, day_ls = Vectorized.calc_and_push_data_to_plasma(
                                                push_return_numpy=True, 
                                                push_stocks_numpy=True
                                            )
            
            n_strats = Vectorized.MultiProcess.compute_signals(recompute_signals)
            # n_strats_cfm = Vectorized.MultiProcess.compute_signals_cfm(recompute_signals)
            # n_strats = 1418

            Vectorized.MultiProcess.compute_wr_re_nt_for_all_strats_os(n_strats, folder=store_folder)
            Vectorized.JoinResults.join_wr_re_nt_data_os(n_strats, stocks_map, src_folder=store_folder, des_folder=result_folder)
        @staticmethod
        def compute_nt_re_wr_cfm():
            ## PARAMS-------------------------------------------------------------------
            # name = 'nt_re_wr_3'
            name = 'environment_3'
            store_folder = f"/data/Tai/{name}_tmp"
            maybe_create_dir(store_folder)

            result_folder = f"/data/Tai/pickles/{name}"
            maybe_create_dir(result_folder)

            recompute_signals = True
            ## CALC  -------------------------------------------------------------------
            
            stocks_map, day_ls = Vectorized.calc_and_push_data_to_plasma(
                                                push_return_numpy=True, 
                                                push_stocks_numpy=True
                                            )
            
            n_strats = Vectorized.MultiProcess.compute_signals(recompute_signals)
            # n_strats_cfm = Vectorized.MultiProcess.compute_signals_cfm(recompute_signals)


            Vectorized.MultiProcess.compute_wr_re_nt_for_all_strats_cfm(n_strats, folder=store_folder)
            Vectorized.JoinResults.join_wr_re_nt_data_cfm(stocks_map, src_folder=store_folder, des_folder=result_folder)

        @staticmethod
        def compute_nt_re_wr_num_days():
            ## PARAMS-------------------------------------------------------------------
            # name = 'nt_re_wr_3_num_days'
            name = 'environment_num_days'

            store_folder = f"/data/Tai/{name}_tmp"
            maybe_create_dir(store_folder)

            result_folder = f"/data/Tai/pickles/{name}"
            maybe_create_dir(result_folder)

            recompute_signals = True
            ## CALC  -------------------------------------------------------------------
            
            stocks_map, day_ls = Vectorized.calc_and_push_data_to_plasma(
                                                push_return_numpy=True, 
                                                push_stocks_numpy=True
                                            )
            
            n_strats = Vectorized.MultiProcess.compute_signals2(recompute_signals)
            n_strats_cfm = Vectorized.MultiProcess.compute_signals_cfm(recompute_signals)


            Vectorized.MultiProcess.compute_num_days_for_all_strats_cfm(n_strats, folder=store_folder)
            Vectorized.JoinResults.join_num_days_data_cfm(stocks_map, src_folder=store_folder, des_folder=result_folder)

        @staticmethod
        def compute_ursi_cfm():
            ## PARAMS-------------------------------------------------------------------
            # name = 'compute_ursi_cfm'
            name = 'environment_ursi_cfm'


            store_folder = f"/data/Tai/{name}_tmp"
            maybe_create_dir(store_folder)

            result_folder = f"/data/Tai/pickles/{name}"
            maybe_create_dir(result_folder)

            recompute_signals = True
            ## CALC  -------------------------------------------------------------------
            
            stocks_map, day_ls = Vectorized.calc_and_push_data_to_plasma(
                                                push_return_numpy=True, 
                                                push_ursi_numpy=True,
                                                push_stocks_numpy=True
                                            )
            
            n_strats = Vectorized.MultiProcess.compute_signals2(recompute_signals)
            n_strats_cfm = Vectorized.MultiProcess.compute_signals_cfm(recompute_signals)


            Vectorized.MultiProcess.compute_ursi_for_all_strats_cfm(n_strats, folder=store_folder)
            Vectorized.JoinResults.join_ursi_data_cfm(src_folder=store_folder, des_folder=result_folder)
        
        @staticmethod
        def compute_label():
            ## PARAMS-------------------------------------------------------------------
            # name = 'new_label'
            name = 'new_label2'

            store_folder = f"/data/Tai/{name}_tmp"
            maybe_create_dir(store_folder)

            result_folder = f"/data/Tai/pickles/{name}"
            maybe_create_dir(result_folder)

            recompute_signals = True
            ## CALC  -------------------------------------------------------------------
            
            stocks_map, day_ls = Vectorized.calc_and_push_data_to_plasma(
                                                push_return_numpy=True, 
                                                push_stocks_numpy=True,
                                                push_label_rudd= True
                                            )
            
            n_strats = Vectorized.MultiProcess.compute_signals_new_cond(recompute_signals)

            Vectorized.MultiProcess.compute_new_label_for_all_strats(n_strats, folder=store_folder)
            Vectorized.JoinResults.join_new_label_data(n_strats, stocks_map, src_folder=store_folder, des_folder=result_folder)


        @staticmethod
        def compute_nt_re_wr():
            ## PARAMS-------------------------------------------------------------------
            name = 'nt_re_wr_3'

            store_folder = f"/data/dang/{name}_tmp"
            maybe_create_dir(store_folder)

            result_folder = f"/home/ubuntu/Dang/pickles/{name}"
            maybe_create_dir(result_folder)

            recompute_signals = True
            ## CALC  -------------------------------------------------------------------
            
            stocks_map, day_ls = Vectorized.calc_and_push_data_to_plasma(
                                                push_return_numpy=True, 
                                                push_stocks_numpy=True
                                            )
            
            n_strats = Vectorized.MultiProcess.compute_signals(recompute_signals)

            Vectorized.MultiProcess.compute_wr_re_nt_for_all_strats(n_strats, folder=store_folder)
            Vectorized.JoinResults.join_wr_re_nt_data(n_strats, stocks_map, src_folder=store_folder, des_folder=result_folder)


        @staticmethod
        def compute_wr_re_nt_yearly():
            ## PARAMS-------------------------------------------------------------------
            name = 'nt_re_wr_yearly'
            recompute_signals: bool = True

            store_folder = f"/data/dang/{name}_tmp"
            maybe_create_dir(store_folder)

            result_folder = f"/home/ubuntu/Dang/pickles/{name}"
            maybe_create_dir(result_folder)

            ## CALC  -------------------------------------------------------------------

            print("---------------------------------------------------------")
            print("Start calculating yearly stats for each combination: ...")
            stocks_map, day_ls = Vectorized.calc_and_push_data_to_plasma(
                                push_stocks_numpy=True, 
                                push_return_numpy=True
                            )
            
            n_strats = Vectorized.MultiProcess.compute_signals(recompute=recompute_signals)
            
            year_map = {}
            year = None
            for idx, d in enumerate(day_ls):
                curr_year = int(d[0:4])
                if curr_year != year:
                    year_map[curr_year] = [idx]
                    if idx != 0:
                        year_map[year].append(idx)
                    year = curr_year
            year_map[year].append(idx+1)

            for year, idxs in year_map.items():
                start_idx, end_idx = idxs

                sub_store_folder = f"{store_folder}/{year}"
                maybe_create_dir(sub_store_folder)

                sub_res_folder = f"{result_folder}/{year}"
                maybe_create_dir(sub_res_folder)

                print(f"Computing stats for {year} ... ", end = ' ')
                Vectorized.MultiProcess.compute_wr_re_nt_2(n_strats, start_idx, end_idx, folder=sub_store_folder)
                print(f"Joining files ... ", end = ' ')
                Vectorized.JoinResults.join_wr_re_nt_data_2(n_strats, stocks_map, src_folder = sub_store_folder, result_folder=sub_res_folder)
                print(f"Finished!")
            print("---------------------------------------------------------")
            print('Done!')

            
        @staticmethod
        def compute_ursi():
            ## PARAMS-------------------------------------------------------------------
            name = 'ursi'
            recompute_signals: bool = False

            store_folder = f"/data/dang/{name}_tmp"
            maybe_create_dir(store_folder)

            result_folder = f"/home/ubuntu/Dang/pickles/{name}"
            maybe_create_dir(result_folder)
            ## CALC  -------------------------------------------------------------------

            stocks_map, day_ls = Vectorized.calc_and_push_data_to_plasma( 
                                                push_stocks_numpy=True,
                                                push_ursi_numpy=True
                                            )
            
            n_strats = Vectorized.MultiProcess.compute_signals(recompute_signals)

            Vectorized.MultiProcess.compute_ursi_for_all_strats(n_strats, folder=store_folder)
            Vectorized.JoinResults.join_ursi_result(n_strats, day_ls, src_folder=store_folder, des_folder=result_folder)


        @staticmethod
        def compute_ru_dd():
            ## PARAMS-------------------------------------------------------------------
            name = 'rudd'
            recompute_signals: bool = False

            store_folder = f"/data/dang/{name}_tmp"
            maybe_create_dir(store_folder)

            result_folder = f"/home/ubuntu/Dang/pickles/{name}"
            maybe_create_dir(result_folder)
            ## CALC  -------------------------------------------------------------------

            stocks_map, day_ls = Vectorized.calc_and_push_data_to_plasma( 
                                                push_stocks_numpy=True,
                                                push_ru_dd_numpy=True
                                            )
            
            n_strats = Vectorized.MultiProcess.compute_signals(recompute=recompute_signals)

            Vectorized.MultiProcess.compute_runup_drawdown_for_all_strats(n_strats, folder=store_folder)
            Vectorized.JoinResults.join_ru_dd_data(n_strats, src_folder=store_folder, des_folder=result_folder)


        @staticmethod
        def compute_sharpe():
            ## PARAMS-------------------------------------------------------------------
            name = 'sharpe'
            recompute_signals: bool = False

            store_folder = f"/data/dang/{name}_tmp"
            maybe_create_dir(store_folder)

            result_folder = f"/home/ubuntu/Dang/pickles/{name}"
            maybe_create_dir(result_folder)
            ## CALC  -------------------------------------------------------------------

            stocks_map, day_ls = Vectorized.calc_and_push_data_to_plasma(
                    push_stocks_numpy=True, 
                    push_return_numpy=True
                )
            
            n_strats = Vectorized.MultiProcess.compute_signals(recompute=recompute_signals)

            Vectorized.MultiProcess.compute_sharpe_for_all_strats(n_strats, folder=store_folder)
            Vectorized.JoinResults.join_sharpe_files(n_strats, stocks_map=stocks_map, src_folder=store_folder, des_folder=result_folder)
            
        @staticmethod
        def compute_uptrend_downtrend():
            ## PARAMS-------------------------------------------------------------------
            name = 'uptrend_downtrend'
            recompute_signals: bool = True

            store_folder = f"/data/dang/{name}_tmp"
            maybe_create_dir(store_folder)

            result_folder = f"/home/ubuntu/Dang/pickles/{name}"
            maybe_create_dir(result_folder)
            ## CALC  -------------------------------------------------------------------
            stocks_map, day_ls = Vectorized.calc_and_push_data_to_plasma(
                push_stocks_numpy=True, 
                push_uptrend_downtrend=True
            )

            n_strats = Vectorized.MultiProcess.compute_signals(recompute=recompute_signals)

            Vectorized.MultiProcess.compute_ut_dt_for_all_strats(n_strats, folder=store_folder)
            Vectorized.JoinResults.join_ut_dt_data(n_strats, stocks_map=stocks_map, src_folder=store_folder, des_folder=result_folder)

        # @staticmethod
        # def compute_peak_trough():
        #     ## PARAMS-------------------------------------------------------------------
        #     name = 'peak_trough'
        #     recompute_signals: bool = True

        #     store_folder = f"/data/dang/{name}_tmp"
        #     maybe_create_dir(store_folder)

        #     result_folder = f"/home/ubuntu/Dang/pickles/{name}"
        #     maybe_create_dir(result_folder)
        #     ## CALC  -------------------------------------------------------------------
        #     stocks_map, day_ls = Vectorized.calc_and_push_data_to_plasma(
        #         push_stocks_numpy=True, 
        #         push_peak_trough= True
        #     )

        #     n_strats = Vectorized.MultiProcess.compute_signals(recompute=recompute_signals)

        #     Vectorized.MultiProcess.compute_ut_dt_for_all_strats(n_strats, folder=store_folder)
        #     Vectorized.JoinResults.join_ut_dt_data(n_strats, stocks_map=stocks_map, src_folder=store_folder, des_folder=result_folder)

    

def test():
    df_ru: pd.DataFrame = pd.read_pickle("/home/ubuntu/Dang/pickles/df_rudd_ru.pkl")
    df_dd: pd.DataFrame = pd.read_pickle("/home/ubuntu/Dang/pickles/df_rudd_dd.pkl")
    df_rudd = df_ru.merge(df_dd, on=['i', 'j'])
    pd.to_pickle(df_rudd, "/home/ubuntu/Dang/pickles/df_rudd.pkl")

    pd.read_pickle("/home/ubuntu/Dang/pickles/sharpe.pkl")
    pd.read_pickle("/home/ubuntu/Dang/pickles/uptrend_downtrend/df_uptrend.pkl")
    pd.read_pickle("/home/ubuntu/Dang/pickles/uptrend_downtrend/df_downtrend.pkl")
        

glob_obj = Globs()
try:
    glob_obj.load_all_data()
except:
    glob_obj.gen_stocks_data()

def test_tai():
    params_df: pd.DataFrame = Vectorized.create_params_sets()
    condition1 = params_df.iloc[53].dropna()
    condition1['use_flag'] = True

    condition2 = params_df.iloc[1325].dropna()
    condition2['use_flag'] = True

    signals1: pd.DataFrame = Conds.compute_any_conds(df, **condition1)
    signals1 = signals1[signals1.index >= '2018_01_01']

    signals2: pd.DataFrame = Conds.compute_any_conds(df, **condition2)
    signals2 = signals2[signals2.index >= '2018_01_01']

    signals = signals1 & signals2
    signals = signals[signals.index >= '2018_01_01']

def test_2():
    params_df: pd.DataFrame = Vectorized.create_params_sets()
    for conds in params_df['function'].unique():
        try:
            condition = params_df[params_df['function']==conds].sample(n=1).iloc[0].dropna()
            condition['use_flag'] = True
            print(condition.to_dict())
            
            signals: pd.DataFrame = Conds.compute_any_conds(df, **condition)
            signals = signals[signals.index >= '2018_01_01']

            print(signals.sum().head(20))
        except:
            print('Error')

    df_stock_count = pd.read_pickle("/home/ubuntu/Dang/pickles/stocks_matched_day.pkl")

def get_from_day(df: pd.DataFrame, from_day, col = None):
    if col is not None:
        df = df[df[col] >= from_day]    
    else:
        df = df[df.index >= from_day]
    return df


class View:
    @staticmethod
    def plot_detecting_peak_result(
                                view_stock, 
                                pct_change = 5,
                                n_bars_before = 15,
                                n_bars_after = 5,
                                pct_change_after = 5):
        # view_stock = 'HPG'
        # df = df_raw.copy()
        # pct_change = 5
        # n_bars_before = 15
        # n_bars_after = 5
        # # margin = 1
        # pct_change_after = 5
            
        df = glob_obj.df_stocks.copy()
        src: pd.DataFrame = df['close']
        high: pd.DataFrame = df['high']
        
        price_change_cond = Conds.price_change_vs_hl(df, 'close', nbars=n_bars_before, low_range=pct_change, use_flag=True)
        
        
        df_matched_before = src == src.rolling(n_bars_before).max()

        df_rollmax = src.rolling(n_bars_after).max().shift(-(n_bars_after))
        # df_matched_after = src >= df_rollmax
        # df_matched_after = high * margin >= high.rolling(n_bars_after).max().shift(-(n_bars_after))
        df_matched_after = src.shift(-n_bars_after) / src < (1 - pct_change_after/100)
        
        df_matched = price_change_cond & df_matched_before & df_matched_after
        
        dfv = src[view_stock].to_frame('close')
        dfv['priceChange'] = dfv['close'].pct_change(periods=n_bars_before).round(6) * 100
        dfv['matchedPC'] = df_matched_before[view_stock]
        dfv['rollmax'] = dfv['close'].rolling(n_bars_after).max()
        dfv['rollmaxShift'] = df_rollmax[view_stock]
        dfv['matchedAfter'] = df_matched_after[view_stock]
        dfv['matched'] = df_matched[view_stock]
        
        price_change_cond2 = Conds.price_change_vs_hl(df, 'close', direction = 'Decrease', nbars=n_bars_before, low_range=pct_change, use_flag=True)
        df_matched_before2 = src == src.rolling(n_bars_before).min()
        df_matched_after2 = src.shift(-n_bars_after) / src > (1 + pct_change_after/100)
        df_matched2 = price_change_cond2 & df_matched_before2 & df_matched_after2

        dfv['matched2'] = df_matched2[view_stock]


        import plotly.graph_objects as go
        import pandas as pd

        df_ohlc = df.xs(view_stock, level = 'stock', axis=1)
        # Create a candlestick chart
        fig = go.Figure(data=[go.Candlestick(x=df_ohlc.index,
                        open=df_ohlc['open'],
                        high=df_ohlc['high'],
                        low=df_ohlc['low'],
                        close=df_ohlc['close'])])
        
        df_matched = dfv[dfv['matched']]
        
        fig.add_trace(go.Scatter(
            x=df_matched.index,
            y=df_matched['close'],
            mode='markers',
            marker=dict(color='blue', size=10, symbol='triangle-down'),
            name='Peak'
        ))
        
        df_matched2 = dfv[dfv['matched2']]
        fig.add_trace(go.Scatter(
            x=df_matched2.index,
            y=df_matched2['close'],
            mode='markers',
            marker=dict(color='yellow', size=10, symbol='triangle-up'),
            name='Trough'
        ))

        # Add titles and labels
        fig.update_layout(
            title='Candlestick Chart',
            xaxis_title='Date',
            yaxis_title='Price',
            xaxis_rangeslider_visible=False
        )

        # Show the figure
        return fig

def create_dash_app():
    from dash import Dash, html, dcc, callback, Output, Input
    import plotly.express as px
    import pandas as pd
    print("Reloading")
    app = Dash()

    app.layout = [
        html.H1(children='Plot', style={'textAlign':'center'}),
        dcc.Dropdown(glob_obj.stocks, 'HPG', id='dropdown-selection'),
        dcc.Graph(id='graph-content')
    ]

    @callback(
        Output('graph-content', 'figure'),
        Input('dropdown-selection', 'value')
    )
    def update_graph(value):
        
        return View.plot_detecting_peak_result(value)
    
    return app


if __name__ == "__main__":
#     glob_obj.load_all_data()
    df_raw = glob_obj.df_stocks.copy()
    df = df_raw.copy()

#     from danglib.utils import show_ram_usage_mb
#     show_ram_usage_mb()
    
#     app = create_dash_app()
#     app.run(debug=True, host='localhost', port=1999)

    # glob_obj.gen_stocks_data()