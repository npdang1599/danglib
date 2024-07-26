"""Import functions"""

import pprint
import logging
from danglib.pylabview2.core_lib import pd, np, Ta, Adapters, Fns, Utils
from numba import njit
from pymongo import MongoClient
import logging
import warnings
from itertools import product
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
        self.function_map = {
            "price_change": Conds.price_change,
            "price_change_vs_hl": Conds.price_change_vs_hl,
            "price_comp_ma": Conds.price_comp_ma,
            "price_gap": Conds.price_gap,
            "price_highest_lowest": Conds.PriceAction.price_highest_lowest,
            "consecutive_conditional_bars": Conds.PriceAction.consecutive_conditional_bars,
        }
        self.sectors = {}

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
    def compute_any_conds(df, function, *args, **kwargs):
        func = glob_obj.function_map[function]
        cond = func(df, *args, **kwargs)
        return cond

    
class Simulator:

    def __init__(
        self,
        func, 
        df_ohlcv: pd.DataFrame,
        params: dict, 
        name: str = ""
    ):
        self.func = func
        self.df = df_ohlcv
        self.params = params
        self.name = name
        self.result = pd.DataFrame

    @staticmethod
    def prepare_data_for_backtesting(
        df: pd.DataFrame, trade_direction="Long", holding_periods=8
    ):
        """Prepare data"""
        df: pd.DataFrame = glob_obj.df_stocks.copy()
        df["entryDay"] = df['day'].shift(-1)
        df["entryType"] = 1 if trade_direction == "Long" else -1
        entry_price: pd.DataFrame = df["open"].shift(-1)
        df["exitDay"] = df["entryDay"].shift(-holding_periods)
        df["exitType"] = df["entryType"] * -1
        exit_price = entry_price.shift(-holding_periods)
        price_change: pd.DataFrame = (exit_price - entry_price).multiply(df['entryType'], axis=0)
        trade_return_tn: pd.DataFrame  = price_change / entry_price * 100
        trade_return_t1: pd.DataFrame  =(entry_price.shift(-1)  / entry_price - 1) * 100
        trade_return_t5: pd.DataFrame  =(entry_price.shift(-5)  / entry_price - 1) * 100
        trade_return_t10: pd.DataFrame =(entry_price.shift(-10) / entry_price - 1) * 100
        trade_return_t15: pd.DataFrame =(entry_price.shift(-15) / entry_price - 1) * 100

        downside: pd.DataFrame = (df["low"].rolling(holding_periods).min().shift(-holding_periods))
        downside = (Ta.min(downside, exit_price) / entry_price -1) * 100

        upside: pd.DataFrame = df["high"].rolling(holding_periods).max().shift(-holding_periods)
        upside = (Ta.max(upside, exit_price) / entry_price -1) * 100
    
        return (
            df, 
            entry_price, 
            exit_price, 
            price_change, 
            trade_return_tn,
            trade_return_t1,
            trade_return_t5,
            trade_return_t10,
            trade_return_t15,
            downside,
            upside
        )
    
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
            profit_factor_arr,
            is_entry_arr,
        )

    def run_combine_multi_conds(
        self, trade_direction="Long", compute_start_time="2018_01_01", holding_periods=8
    ):
        df = self.df
        func = self.func
        params = self.params
        name = self.name

        df = glob_obj.df_stocks.copy()
        func = Conds.price_change
        params = {
            'lower_thres':5,
            'use_flag':True
        }
        
        trade_direction="Long"
        compute_start_time="2018_01_01"
        holding_periods=15
        df = glob_obj.df_stocks.copy()
        func = Conds.price_change
        params = {
            "lower_thres":5,
            "use_flag":True
        }

        (df, 
        entry_price, 
        exit_price, 
        price_change, 
        trade_return_tn,
        trade_return_t1,
        trade_return_t5,
        trade_return_t10,
        trade_return_t15,
        downside,
        upside) = Simulator.prepare_data_for_backtesting(
            df,
            trade_direction=trade_direction,
            holding_periods=holding_periods,
        )
        
        signals: pd.DataFrame = func(df, **params)

        signals.loc[signals.index < compute_start_time] = signals.loc[signals.index < compute_start_time].map(lambda x: False)
        res = []
        from tqdm import tqdm
        for s in tqdm(signals.columns.tolist()):

            s_df = df.xs(s, 1, level=1)
            s_df = s_df[s_df['close'].notna()].copy()
            s_df['stock'] = s
            s_df['signal'] = s_df.index.map(signals[s])
            s_df['entryPrice'] = s_df.index.map(entry_price[s])
            s_df['exitPrice'] = s_df.index.map(exit_price[s])
            s_df['priceChange'] = s_df.index.map(price_change[s])
            
            s_df['return'] = s_df.index.map(trade_return_tn[s])
            s_df['return1'] = s_df.index.map(trade_return_t1[s])
            s_df['return5'] = s_df.index.map(trade_return_t5[s])
            s_df['return10'] = s_df.index.map(trade_return_t10[s])
            s_df['return15'] = s_df.index.map(trade_return_t15[s])
            s_df['upside'] = s_df.index.map(upside[s])
            s_df['downside'] = s_df.index.map(downside[s])


            s_price_changes = s_df["priceChange"].values
            s_returns = s_df["return"].values
            s_returns1 = s_df["return1"].values
            s_returns5 = s_df["return5"].values
            s_returns10 = s_df["return10"].values
            s_returns15 = s_df["return15"].values
            s_signals = s_df["signal"].values
            s_upsides = s_df["upside"].values
            s_downsides = s_df["downside"].values
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
                profit_factor_arr,
                is_entry_arr,
            ) = Simulator.compute_trade_stats(
                s_price_changes, 
                s_returns, 
                s_returns1, 
                s_returns5, 
                s_returns10, 
                s_returns15, 
                s_signals, 
                s_upsides, 
                s_downsides, 
                holding_periods
            )
            
            s_df["numTrade"] = num_trades_arr
            s_df["winrate"] = winrate_arr
            s_df["winrateT1"] = winrate1_arr
            s_df["winrateT5"] = winrate5_arr
            s_df["winrateT10"] = winrate10_arr
            s_df["winrateT15"] = winrate15_arr
            s_df["avgReturn"] = avg_returns_arr
            s_df["avgReturnT1"] = avg_returns1_arr
            s_df["avgReturnT5"] = avg_returns5_arr
            s_df["avgReturnT10"] = avg_returns10_arr
            s_df["avgReturnT15"] = avg_returns15_arr
            s_df["avgUpside"] = avg_upside_arr
            s_df["avgDownside"] = avg_downside_arr
            s_df["profitFactor"] = profit_factor_arr
            s_df["isEntry"] = is_entry_arr
            s_df["isTrading"] = trading_status


            s_df["matched"] = np.where(s_df["signal"], 1, np.NaN)
            s_df[
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
                    "avgDownside"
                ]
            ] = s_df[
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
                    "avgDownside"
                ]
            ].ffill().fillna(0)

            res.append(s_df)

        df_res =pd.concat(res, axis=0)

        t = df_res[df_res['stock'] == 'HPG']

        result = t[
            [
                "stock",
                "numTrade",
                "winrate",
                "profitFactor",
                "avgReturn",
                "avgUpside",
                "avgDownside",
                "isEntry",
                "matched",
                "isTrading",
            ]
        ].iloc[-1]
        
        return result, df_res


class Vectorized:
    @staticmethod
    def create_params_sets():
# %%
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
            }

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

        return params_df

        


# %% 


    @staticmethod
    def compute_conditions(df, params):

        df = df_raw.copy()

        params_df:pd.DataFrame = Vectorized.create_params_sets()

        sig_dic = {}
        for idx, params in params_df.iterrows():
            try:
                params = params.dropna().to_dict()

                signals: pd.DataFrame = Conds.compute_any_conds(df, **params)

                sig_dic[idx] = signals
            except Exception as e:
                print(params)
                print(e)



glob_obj = Globs()
glob_obj.load_stocks_data()
glob_obj.load_vnindex()
glob_obj.load_sectors_data()

df_raw = glob_obj.df_stocks.copy()