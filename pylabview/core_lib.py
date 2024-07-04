"""import libraries"""

import logging
import os
from datetime import datetime as dt
import pandas as pd
import numpy as np
from pymongo import MongoClient
from vnstock3 import Vnstock
from numba import njit

if "ACCEPT_TC" not in os.environ:
    os.environ["ACCEPT_TC"] = "tôi đồng ý"

today = dt.now().strftime(format="%Y-%m-%d")


class Adapters:
    """Adapter functions to get data from resources"""

    @staticmethod
    def load_stocks_data_from_pickle():
        """Load stocks data from pickle file"""
        return pd.read_pickle("/home/ubuntu/Dang/data/map_income_df2.pickle")

    @staticmethod
    def map_net_income(df: pd.DataFrame, symbol: str):
        """Map DataFrame with Quarterly Net Income data

        Args:
            df (pd.DataFrame): input time-series dataframe
            symbol (str): stock symbol

        Returns:
            pd.DataFrame: Quarterly Net Income mapped dataframe
        """
        df = Utils.compute_quarter_day_map(df)
        df_ni = Adapters.load_quarter_netincome_from_vnstocks(symbol)
        df["netIncome"] = df["mapYQ"].map(df_ni["netIncome"])
        return df

    @staticmethod
    def load_quarter_netincome_from_vnstocks(symbol: str):
        """Load quarter Net Income data from Vnstocks API, source: VCI

        Args:
            symbol (str): stock symbol

        Returns:
            pd.DataFrame: Net Income dataframe
            columns: ['ticker', 'year', 'quarter', 'netIncome']
        """
        stock = Vnstock().stock(symbol=symbol, source="VCI")
        df: pd.DataFrame = stock.finance.income_statement(period="quarter")
        df = df[
            ["ticker", "yearReport", "lengthReport", "Net Profit/Loss before tax"]
        ].copy()
        df.columns = ["ticker", "year", "quarter", "netIncome"]
        df['netIncome'] = df['netIncome'].fillna(0)
        df["mapYQ"] = df["year"] * 10 + df["quarter"]
        df["mapYQ"] = np.where(df["quarter"] == 4, df["mapYQ"] + 7, df["mapYQ"] + 1)
        df = df.set_index("mapYQ")
        return df

    @staticmethod
    def get_stocks_ohlcv_from_db_cafef(stocks: list, from_day: str = "2018_01_01"):
        """load stocks' ohlcv data from db

        Args:
            stocks (list): list of stocks

        Returns:
            pd.DataFrame: ohlcv data of stocks
        """
        db = MongoClient("ws", 27022)["stockdata"]
        col = db["price_cafef_data"]

        df = pd.DataFrame(
            list(
                col.find(
                    {"symbol": {"$in": stocks}, "day": {"$gte": from_day}},
                    {
                        "_id": 0,
                        "adjusted_close": 1,
                        "open": 1,
                        "high": 1,
                        "low": 1,
                        "totalMatchVal": 1,
                        "day": 1,
                        "symbol": 1,
                    },
                )
            )
        )
        df = df.rename(
            columns={
                "adjusted_close": "close",
                "totalMatchVal": "volume",
                "symbol": "stock",
            }
        )
        df = df.sort_values("day")
        if len(df) != 0:
            df = df.drop_duplicates(keep="first")
            return_stocks = df.loc[:, "stock"].unique()
            miss_stocks = [s for s in stocks if s not in return_stocks]
        else:
            return_stocks = 0
        miss_stocks_len = len(miss_stocks)
        if miss_stocks_len != 0:
            warning_msg = (
                f"There is(are) {miss_stocks_len} stock(s) not in db: {miss_stocks}"
            )
            logging.warning(warning_msg)
        df = df.reset_index(drop=True)
        return df

    @staticmethod
    def get_stocks_ohlcv_from_db(stocks: list):
        """load stocks' ohlcv data from db

        Args:
            stocks (list): list of stocks

        Returns:
            pd.DataFrame: ohlcv data of stocks
        """
        db = MongoClient("ws", 27022)["stockdata"]
        col = db["price_data"]

        df = pd.DataFrame(
            list(col.find({"stock": {"$in": stocks}, "source": "TCBS"}, {"_id": 0}))
        )
        if len(df) != 0:
            df = df.drop_duplicates(keep="first")
            return_stocks = df.loc[:, "stock"].unique()
            miss_stocks = [s for s in stocks if s not in return_stocks]
        else:
            return_stocks = 0
        miss_stocks_len = len(miss_stocks)
        if miss_stocks_len != 0:
            warning_msg = (
                f"There is(are) {miss_stocks_len} stock(s) not in db: {miss_stocks}"
            )
            logging.warning(warning_msg)
        df = df[df["day"] >= "2018_01_01"].reset_index(drop=True)
        return df

    @staticmethod
    def get_stock_from_vnstock(ticker: str, from_day: str = "2018_01_01"):
        """load stock's ohlcv data from vnstock

        Args:
            ticker (str): stock symbol
            from_day (str): data start day

        Returns:
            pd.DataFrame: ohlcv data
        """
        stock = Vnstock().stock(symbol=ticker, source="VCI")
        df: pd.DataFrame = stock.quote.history(start="2014-01-01", end=today)
        df = df.rename(columns={"time": "day"})
        df["day"] = df["day"].astype(str).str.replace("-", "_")
        df = df[df["day"] >= from_day].reset_index(drop=True)
        return df

    @staticmethod
    def get_stocks_from_db_ssi(stocks, from_day: str = "2018_01_01"):
        """load stocks' ohlcv data from db SSI

        Args:
            stocks (list): list of stocks
            from_day (str): data start day
        Returns:
            pd.DataFrame: ohlcv data of stocks
        """
        db = MongoClient("ws", 27022)["stockdata"]
        col = db["price_curated"]

        # df = pd.DataFrame(list(col.find({},{"_id":0}).limit(20)))
        # df.columns

        df = pd.DataFrame(
            list(
                col.find(
                    {"symbol": {"$in": stocks}, "day": {"$gte": from_day}},
                    {"_id": 0},
                )
            )
        )
        df = df.rename(
            columns={
                # "openPrice": "open",
                # "highestPrice": "high",
                # "lowestPrice": "low",
                "adjusted_close": "close",
                "totalMatchVal": "volume",
                "symbol": "stock",
            }
        )
        df = df.sort_values("day")
        df = df[df["volume"] > 0].copy()

        # ls = col.distinct('symbol')
        # "VN30" in ls

        if len(df) != 0:
            df = df.drop_duplicates(keep="first")
            return_stocks = df.loc[:, "stock"].unique()
            miss_stocks = [s for s in stocks if s not in return_stocks]
        else:
            return_stocks = 0
        miss_stocks_len = len(miss_stocks)
        if miss_stocks_len != 0:
            warning_msg = (
                f"There is(are) {miss_stocks_len} stock(s) not in db: {miss_stocks}"
            )
            logging.warning(warning_msg)
        df = df.reset_index(drop=True)
        return df

    @staticmethod
    def get_stocks_from_db_ssi2(stocks, from_day: str = "2017_01_01"):
        """load stocks' ohlcv data from db SSI

        Args:
            stocks (list): list of stocks
            from_day (str): data start day
        Returns:
            pd.DataFrame: ohlcv data of stocks
        """
        db = MongoClient("ws", 27022)["stockdata"]
        col = db["price_tradingview_2"]

        # df = pd.DataFrame(list(col.find({},{"_id":0}).limit(20)))
        # df.columns

        df = pd.DataFrame(
            list(
                col.find(
                    {"symbol": {"$in": stocks}, "day": {"$gte": from_day}},
                    {"_id": 0},
                )
            )
        )
        df = df.rename(
            columns={
                # "openPrice": "open",
                # "highestPrice": "high",
                # "lowestPrice": "low",
                "adjusted_close": "close",
                "value": "volume",
                "symbol": "stock",
            }
        )
        df = df.sort_values("day")
        df = df[df["volume"] > 0].copy()

        # ls = col.distinct('symbol')
        # "VN30" in ls

        if len(df) != 0:
            df = df.drop_duplicates(keep="first")
            return_stocks = df.loc[:, "stock"].unique()
            miss_stocks = [s for s in stocks if s not in return_stocks]
        else:
            return_stocks = 0
        miss_stocks_len = len(miss_stocks)
        if miss_stocks_len != 0:
            warning_msg = (
                f"There is(are) {miss_stocks_len} stock(s) not in db: {miss_stocks}"
            )
            logging.warning(warning_msg)
        df = df.reset_index(drop=True)
        return df

    @staticmethod
    def get_stocks_beta_group():
        """Get stocks list and group by Beta from excel file

        Returns:
            stocks (list): stocks list,
            stocks_groups_map (dict): map stock with beta group
        """
        path = "/home/ubuntu/anaconda3/envs/dang/lib/python3.10/danglib/pylabview/All Ticker_Beta_May2024.xlsx"
        df = pd.read_excel(path, sheet_name="Sheet3")
        stocks = df["Ticker"].tolist()

        group_name_map = {1: "Super High Beta", 2: "High Beta", 3: "Medium", 4: "Low"}

        df["group_name"] = df["Group"].map(group_name_map)
        stocks_groups_map = dict(zip(df["Ticker"], df["group_name"]))
        return stocks, stocks_groups_map
    
    @staticmethod
    def load_income_statement_quarter_VCI_from_db():
        db = MongoClient("ws", 27022)["stockdata"]
        col = db['VCI_income_statement_quarter']
        df = pd.DataFrame(list(col.find({},{"_id":0})))
        return df

    @staticmethod
    def get_stocks():
        """get stocks' symbol"""
        stock_nest = [
            [
                "HNX:SHS",
                "HNX:VGS",
                "UPCOM:SBS",
                "HNX:MBS",
                "HOSE:VIX",
                "HNX:CEO",
                "HNX:DTD",
                "HOSE:CTS",
                "HOSE:ORS",
                "HOSE:GEX",
                "HOSE:VND",
                "HOSE:VCI",
                "HOSE:AGR",
                "HOSE:FTS",
                "HOSE:BSI",
                "HOSE:DIG",
                "HOSE:DXG",
                "HOSE:CII",
                "HOSE:PDR",
            ],
            [
                "HOSE:HTN",
                "HOSE:DGW",
                "HNX:L14",
                "HOSE:VDS",
                "HOSE:NKG",
                "HOSE:NVL",
                "HOSE:HSG",
                "HOSE:SSI",
                "HOSE:VPG",
                "UPCOM:AAS",
                "HOSE:LCG",
                "HOSE:HDC",
                "HOSE:DPG",
                "HOSE:FCN",
                "HOSE:GIL",
                "HOSE:KBC",
                "HOSE:VCG",
                "HOSE:IJC",
                "HOSE:NLG",
            ],
            [
                "HNX:PLC",
                "HOSE:VGC",
                "HOSE:DBC",
                "HOSE:TCH",
                "UPCOM:DDV",
                "HOSE:PC1",
                "HOSE:BCG",
                "HOSE:ASM",
                "HOSE:IDI",
                "HOSE:ANV",
                "HOSE:KHG",
                "HOSE:SZC",
                "HOSE:PET",
                "HOSE:DXS",
                "HOSE:TCD",
                "UPCOM:C4G",
                "HOSE:CRE",
                "HOSE:HHV",
                "HNX:PVC",
            ],
            [
                "HOSE:HAH",
                "HNX:TNG",
                "HOSE:CTD",
                "HOSE:PVD",
                "HOSE:SCR",
                "HOSE:PVT",
                "HNX:TIG",
                "HOSE:HHS",
                "HOSE:MWG",
                "HOSE:DGC",
                "HOSE:DCM",
                "HNX:PVS",
                "HOSE:VTP",
                "HOSE:AAA",
                "HOSE:MSN",
                "HNX:IDC",
                "UPCOM:BCR",
                "HOSE:VSC",
                "HOSE:NTL",
            ],
            [
                "HOSE:STB",
                "HOSE:EIB",
                "HOSE:HDG",
                "HOSE:PAN",
                "HOSE:DPM",
                "HOSE:SHB",
                "HOSE:MSB",
                "HOSE:KDH",
                "HOSE:TCB",
                "HOSE:HPG",
                "HOSE:VRE",
                "HOSE:VHC",
                "HOSE:VHM",
                "UPCOM:BSR",
                "HNX:NVB",
                "HOSE:EVF",
                "HOSE:TV2",
                "HOSE:LHG",
                "HOSE:CTR",
            ],
            [
                "HOSE:TPB",
                "HOSE:VIB",
                "HOSE:VPB",
                "UPCOM:VGT",
                "HOSE:CTG",
                "HOSE:HT1",
                "HOSE:VIC",
                "HOSE:MBB",
                "HOSE:GMD",
                "HOSE:APH",
                "HOSE:LPB",
                "UPCOM:LTG",
                "HOSE:BID",
                "HNX:VCS",
                "HOSE:OCB",
                "HOSE:PLX",
                "HOSE:AGG",
                "HOSE:HAX",
                "HOSE:POW",
            ],
            [
                "HOSE:BMP",
                "HOSE:GEG",
                "HOSE:FPT",
                "UPCOM:OIL",
                "HOSE:DRC",
                "HOSE:ACB",
                "HOSE:FRT",
                "HOSE:DHC",
                "HOSE:NT2",
                "HOSE:REE",
                "UPCOM:ABB",
                "HOSE:BCM",
                "HOSE:HVN",
                "HOSE:GAS",
                "HOSE:BVH",
                "HOSE:PNJ",
                "HNX:VFS",
                "HOSE:SAM",
                "HOSE:HDB",
            ],
            [
                "HOSE:CMG",
                "HOSE:SAB",
                "HOSE:BAF",
                "HNX:VC3",
                "HOSE:NAB",
                "HOSE:VNM",
                "UPCOM:QNS",
                "HOSE:VJC",
                "UPCOM:DVN",
                "HOSE:VCB",
                "HOSE:TCM",
                "HOSE:PTB",
                "HOSE:SJS",
                "HOSE:VPI",
                "HOSE:SSB",
                "UPCOM:VEA",
                "UPCOM:ACV",
                "HOSE:TDM",
                "HNX:HTP",
            ],
            [
                "HOSE:SHI",
                "HOSE:KDC",
                "HOSE:DBD",
                "HOSE:CTF",
                "HOSE:NAB",
                "HOSE:VNM",
                "UPCOM:QNS",
                "HOSE:VJC",
                "UPCOM:DVN",
                "HOSE:VCB",
                "HOSE:TCM",
                "HOSE:PTB",
                "HOSE:SJS",
                "HOSE:VPI",
                "HOSE:SSB",
                "UPCOM:VEA",
                "UPCOM:ACV",
                "HOSE:TDM",
                "HNX:HTP",
            ],
        ]
        stocks: list[str] = []
        for sub in stock_nest:
            stocks += sub
        stocks = [i.split(":")[1] for i in stocks]
        return list(set(stocks))


class Ta:
    """Technical analysis"""

    @staticmethod
    def highest(src: pd.Series, length: int):
        """Highest value for a given period.

        Args:
            src (pd.Series): Series of values to process.
            length (_type_): Number of bars.

        Returns:
            pd.Series: Highest value in the period
        """
        return src.rolling(length).max()

    @staticmethod
    def is_highest(src: pd.Series, length: int):
        """Check if this is highest value for a given period.

        Args:
            src (pd.Series): Series of values to process.
            length (_type_): Number of bars.

        Returns:
            pd.Series: If this is highest value of given period
        """
        return src.rolling(length).max() == src

    @staticmethod
    def lowest(src: pd.Series, length: int):
        """Lowest value for a given period.

        Args:
            src (pd.Series): Series of values to process.
            length (_type_): Number of bars.

        Returns:
            pd.Series: Lowest value in the period
        """
        return src.rolling(length).min()

    @staticmethod
    def is_lowest(src: pd.Series, length: int):
        """Check if is lowest value for a given period.

        Args:
            src (pd.Series): Series of values to process.
            length (_type_): Number of bars.

        Returns:
            pd.Series: if is lowest value for a given period
        """
        return src.rolling(length).min() == src

    @staticmethod
    def sma(src: pd.Series, length):
        """The sma function returns the moving average,
        that is the sum of last y values of x, divided by y.

        Args:
            src (pd.Series): Series of values to process.
            length (_type_): Number of bars.

        Returns:
            pd.Series: Simple moving average of source for length bars back.
        """
        return src.rolling(length).mean()

    @staticmethod
    def ema(src: pd.Series, length):
        """The ema function returns the exponentially weighted moving average.
        In ema weighting factors decrease exponentially.
        It calculates by using a formula:
        EMA = alpha * source + (1 - alpha) * EMA[1], where alpha = 2 / (length + 1).

        Args:
            src (pd.Series): Series of values to process.
            length (int): Number of bars (length).

        Returns:
            pd.Series: Exponential moving average of source with alpha = 2 / (length + 1).
        """
        return src.ewm(span=length, adjust=False).mean()

    @staticmethod
    def rma(src: pd.Series, length):
        """Moving average used in RSI.
        It is the exponentially weighted moving average with alpha = 1 / length.

        Args:
            src (pd.Series): Series of values to process.
            length (_type_): Number of bars (length).

        Returns:
            pd.Series: Exponential moving average of source with alpha = 1 / length.
        """
        return src.ewm(alpha=1 / length, adjust=False).mean()

    @staticmethod
    def ma(src: pd.Series, length, ma_type):
        """
        if ma_type == "EMA":
            return Ta.ema(src, length)
        if ma_type == "SMA":
            return Ta.sma(src, length)
        if ma_type == "RMA":
            return Ta.rma(src, length)
        """

        ma_dict = {"EMA": Ta.ema, "SMA": Ta.sma, "RMA": Ta.rma}

        return ma_dict[ma_type](src, length)

    @staticmethod
    def crossover(src1: pd.Series, src2: pd.Series):
        """The source1-series is defined as having crossed over source2-series if,
        on the current bar, the value of source1 is greater than the value of source2,
        and on the previous bar, the value of source1 was less than or
        equal to the value of source2

        Args:
            src1 (pd.Series): First data series.
            src2 (pd.Series): Second data series.

        Returns:
            pd.Series: true if source1 crossed over source2 otherwise false.
        """
        return (src1 >= src2) & (src1.shift(1) < src2.shift(1))

    @staticmethod
    def crossunder(src1: pd.Series, src2: pd.Series):
        """The source1-series is defined as having crossed under source2-series if,
        on the current bar, the value of source1 is less than the value of source2,
        and on the previous bar, the value of source1 was greater than or
        equal to the value of source2.

        Args:
            src1 (pd.Series): First data series.
            src2 (pd.Series): Second data series.

        Returns:
            pd.Series: true if source1 crossed under source2 otherwise false.
        """
        return (src1 <= src2) & (src1.shift(1) > src2.shift(1))

    @staticmethod
    def stdev(source: pd.Series, length: int):
        """Calculating standard deviation

        Args:
            source (pd.Series): Series of values to process
            length (int): Number of bars (length)

        Returns:
            pd.Series: source.rolling(length).std()
        """
        return source.rolling(length).std(ddof=0)

    @staticmethod
    def correlation(series1: pd.Series, series2: pd.Series, length: int):
        """Calculate rolling correlation between two series."""
        return series1.rolling(window=length).corr(series2)

    @staticmethod
    def squeeze(
        df: pd.DataFrame,
        src_name: str = "close",
        bb_length: int = 20,
        length_kc: int = 20,
        mult_kc: float = 1.5,
        use_true_range: bool = True,
    ):
        """Calculation squeeze

        Args:
            df (pd.DataFrame): ohlcv dataframe
            src_name (str, optional): source name (open, high, low, close). Defaults to 'close'.
            bb_length (int, optional): BB length. Defaults to 20.
            length_kc (int, optional): KC length. Defaults to 20.
            mult_kc (float, optional): KC mult factor. Defaults to 1.5.
            use_true_range (bool, optional): Use true range (KC) or not . Defaults to True.

        Returns:
            sqz_on (pd.Series): Series of squeeze on or not
            sqz_off (pd.Series): Series of squeeze off or not
            no_sqz (pd.Series): Series of no squeeze (no on and no off)
        """
        p_h = df["high"]
        p_l = df["low"]
        p_c = df["close"]
        src = df[src_name]

        basic = Ta.sma(src, bb_length)
        dev = mult_kc * Ta.stdev(src, bb_length)
        upper_bb = basic + dev
        lower_bb = basic - dev

        sqz_ma = Ta.sma(src, length_kc)

        sqz_range = (
            Math.max([p_h - p_l, abs(p_h - p_c.shift(1)), abs(p_l - p_c.shift(1))])
            if use_true_range
            else (p_h - p_l)
        )

        rangema = Ta.sma(sqz_range, length_kc)
        upper_kc = sqz_ma + rangema * mult_kc
        lower_kc = sqz_ma - rangema * mult_kc

        sqz_on = (lower_bb > lower_kc) & (upper_bb < upper_kc)
        sqz_off = (lower_bb < lower_kc) & (upper_bb > upper_kc)
        no_sqz = pd.Series(np.where(sqz_on | sqz_off, False, True))

        return sqz_on, sqz_off, no_sqz

    @staticmethod
    def ursi(
        src: pd.Series,
        length: int = 14,
        smo_type1: str = "RMA",
        smooth: int = 14,
        smo_type2: str = "EMA",
    ):
        """Ultimate RSI

        Args:
            src (pd.Series): Input source of the indicator (open, high, low, close)

            length (int, optional): Calculation period of the indicator. Defaults to 14.

            smo_type1 (str, optional): Smoothing method used for the calculation of the indicator.
            Defaults to 'RMA'.

            smooth (int, optional): Degree of smoothness of the signal line. Defaults to 14.

            smo_type2 (str, optional): Smoothing method used to calculation the signal line.
            Defaults to 'EMA'.

        Returns:
            arsi (pd.Series): ursi line
            signal (pd.Series): ursi's signal line
        """
        
        upper = Ta.highest(src, length)
        lower = Ta.lowest(src, length)
        r = upper - lower
        d = src.diff()

        diff = np.where(upper > upper.shift(1), r, np.where(lower < lower.shift(1), -r, d))
        diff = pd.Series(diff, index=src.index)

        num = Ta.ma(diff, length, smo_type1)
        den = Ta.ma(diff.abs(), length, smo_type1)
        arsi = (num / den) * 50 + 50

        signal = Ta.ma(arsi, smooth, smo_type2)

        return arsi, signal

    @staticmethod
    def macd_old(
        df: pd.DataFrame,
        src_name: str = "close",
        r2_period: int = 20,
        fast: int = 10,
        slow: int = 20,
        signal_length: int = 9,
    ):
        """Calculate MACD"""

        src = df[src_name]
        bar_index = pd.Series(df.index)

        # Create macd1 with all zero
        macd = Utils.new_1val_series(0, src)

        # lag = (signal_length - 1) / 2
        a1 = 2 / (fast + 1)
        a2 = 2 / (slow + 1)

        r2 = 0.5 * pow(Ta.correlation(src, bar_index, r2_period), 2) + 0.5
        k = r2 * ((1 - a1) * (1 - a2)) + (1 - r2) * ((1 - a1) / (1 - a2))

        macd = (
            (src - Utils.nz(src.shift(1))) * (a1 - a2)
            + (-a2 - a1 + 2) * Utils.nz(macd.shift(1))
            - k * Utils.nz(macd.shift(2))
        )
        signal = Ta.ema(macd, signal_length)

        return macd, signal
    
    @staticmethod
    def macd(
        df: pd.DataFrame,
        src_name: str = "close",
        r2_period: int = 20,
        fast: int = 10,
        slow: int = 20,
        signal_length: int = 9,
    ):
        """Calculate MACD"""

        src = df[src_name]
        bar_index = np.arange(len(src))

        a1 = 2 / (fast + 1)
        a2 = 2 / (slow + 1)

        correlation = src.rolling(r2_period).corr(pd.Series(bar_index))
        r2 = 0.5 * correlation**2 + 0.5
        K = r2 * ((1 - a1) * (1 - a2)) + (1 - r2) * ((1 - a1) / (1 - a2))

        np_macd = np.zeros(len(src)) * np.nan
        
        for i in range(2, len(src)):
            np_macd[i] = (
                (src[i] - np.nan_to_num(src[i - 1])) * (a1 - a2)
                + (-a2 - a1 + 2) * np.nan_to_num(np_macd[i - 1])
                - K[i] * (np.nan_to_num(np_macd[i - 2]))
            )
        
        macd = pd.Series(np_macd, index=bar_index)
        signal = Ta.ema(macd, signal_length)

        return macd, signal

    @staticmethod
    def bbwp_old(
        df: pd.DataFrame,
        src_name: str = "close",
        basic_type: str = "SMA",
        bbwp_len: int = 13,
        bbwp_lkbk: int = 128,
    ):
        """bbwp"""
        _price = df[src_name]

        _basic = Ta.ma(_price, bbwp_len, basic_type)
        _dev = Ta.stdev(_price, bbwp_len)
        _bbw = (_basic + _dev - (_basic - _dev)) / _basic

        # _bbw_sum = (_bbw.rolling(bbwp_lkbk).rank() - 1) / (bbwp_lkbk - 1)
        _bbw_sum = (_bbw.rolling(bbwp_lkbk + 1).rank() - 1) / bbwp_lkbk

        return _bbw_sum * 100
    
    @staticmethod
    def bbwp(
        df: pd.DataFrame,
        src_name: str = "close",
        basic_type: str = "SMA",
        bbwp_len: int = 13,
        bbwp_lkbk: int = 128,
    ):
        """bbwp"""
        _price = df[src_name]

        _basic = Ta.ma(_price, bbwp_len, basic_type)
        _dev = Ta.stdev(_price, bbwp_len)
        _bbw = (_basic + _dev - (_basic - _dev)) / _basic

        # _bbw_sum = (_bbw.rolling(bbwp_lkbk).rank() - 1) / (bbwp_lkbk - 1)
        # _bbw_sum = (_bbw.rolling(bbwp_lkbk + 1).rank() - 1) / bbwp_lkbk
        index = df.index
        bbwp_denominator = np.where(index < bbwp_lkbk, index, bbwp_lkbk)
        _bbw_sum = (_bbw.rolling(bbwp_lkbk + 1, min_periods= bbwp_len).rank() - 1) / bbwp_denominator

        return _bbw_sum * 100

    @staticmethod
    def bbpctb(
        df: pd.DataFrame, src_name: str = "close", length: int = 20, mult: float = 2
    ):
        """bbpctb"""
        src = df[src_name].copy()
        basic = Ta.sma(src, length)
        dev = mult * Ta.stdev(src, length)
        upper = basic + dev
        lower = basic - dev
        bbpctb = (src - lower) / (upper - lower) * 100

        return bbpctb
    
class VTa:
    """TA functions used for vector calculations"""

    @staticmethod
    def min(src1:pd.DataFrame, src2:pd.DataFrame):
        return pd.DataFrame(np.minimum(src1.values, src2.values), columns=src1.columns, index=src1.index)

    @staticmethod
    def max(src1:pd.DataFrame, src2:pd.DataFrame):
        return pd.DataFrame(np.maximum(src1.values, src2.values), columns=src1.columns, index=src1.index            )


class Math:
    """mathematical functions for pandas Series"""

    @staticmethod
    def max(series_ls: list[pd.Series]):
        """To calculate the maximum value for each row from multiple Pandas Series.

        Args:
            series_ls (list): A sequence of series to use in the calculation.

        Returns:
            pd.Series: The series of maximum values for each row of the input series
        """
        return pd.concat(series_ls, axis=1).max(axis=1)

    @staticmethod
    def sum(series: pd.Series, length: int):
        """The sum function returns the sliding sum of last y values of x.

        Args:
            series (pd.Series): Series of values to process.
            length (int): Number of bars (length).

        Returns:
            pd.Series: Sum of source for length bars back.
        """
        return series.rolling(length).sum()

    @staticmethod
    def pct_change(series: pd.Series, periods: int = 1):
        """Fractional change between the current and a prior element.

        Computes the fractional change from the immediately previous row by
        default. This is useful in comparing the fraction of change in a time
        series of elements.

        Args:
            series (pd.Series): Series of values to process.

            periods (int): Periods to shift for forming percent change. Default 1

        Returns:
            pd.Series: (series - series.shift(periods=periods)) / abs(series.shift(periods=periods))
        """

        return (series - series.shift(periods=periods)) / abs(
            series.shift(periods=periods)
        )


class Utils:
    """Ultilities"""

    @staticmethod
    def two_lines_position(src1: pd.Series, src2: pd.Series) :
        pos = np.select(
            [
                Ta.crossover(src1, src2), Ta.crossunder(src1, src2), (src1 > src2) & 
                ( ~ Ta.crossover(src1,src2)), (src1 < src2) & (~ Ta.crossunder(src1, src2))
            ],
            ['crossover', 'crossunder', 'above', 'below']
        )
        return pos

    @staticmethod
    def calc_percentage_change(a, b):
        """Calculate percentage change between b and a
        formula: (b - a) / a * 100
        """
        return (b - a) / abs(a) * 100

    @staticmethod
    def count_changed(src: pd.Series, num_bars: int, direction: str):
        """Count the number of bars that increased (decreased) within the last n bars.

        Args:
            src (pd.Series): series for calculating
            num_bar (int): number of latest bars needed to be checked
            direction (str): "Increase" or "Decrease"

        Returns:
            pd.Series: A series showing the number of bars that
            increased (decreased) within the last n bars
        """

        diff = src.diff()
        matched: pd.Series = diff > 0 if direction == "Increase" else diff < 0
        count = matched.rolling(num_bars, min_periods=1).sum()

        return count

    @staticmethod
    def use_flag(cond_series: pd.Series, use_flag: bool = True):
        """If use_flag is on, condition results remain unchanged,
        if it is off, change result to True

        Args:
            cond_series (pd.Series): bool Series
            use_flag (bool, optional): True or False. Defaults to True.

        Returns:
            pd.Series: unchanged or all True
        """
        return np.where(use_flag, cond_series, True)

    @staticmethod
    def in_range(src: pd.Series, lower_thres: float, upper_thres: float, equal=True):
        """Check if the source value is within the range from lower to upper.

        Args:
            src (pd.Series): source, series of value needed to compare
            lower_thres (float): lower threshold
            upper_thres (float): upper threshold

        Returns:
            pd.Series: (lower_thres < src) & (src < upper_thres)
        """
        comp_src = src.round(6)
        if equal:
            return (lower_thres <= comp_src) & (comp_src <= upper_thres)
        
        return (lower_thres < comp_src) & (comp_src < upper_thres)

    @staticmethod
    def count_consecutive(series: pd.Series):
        """_summary_

        Args:
            series (pd.Series): _description_

        Returns:
            _type_: _description_
        """
        # Create an accumulating group, increasing the group when encountering a False value
        # groups = (series == False).cumsum()
        groups = np.where(series, False, True).cumsum()

        # Calculate cumsum within each group
        result = series.groupby(groups).cumsum()

        return result

    @staticmethod
    def nz(series: pd.Series):
        """Fill NaN value is the Series with 0"""
        return series.fillna(0)

    @staticmethod
    def new_1val_series(value, series_to_copy_index: pd.Series):
        """Create an one-value series replicated another series' index"""
        return pd.Series(value, index=series_to_copy_index.index)

    @staticmethod
    def compute_quarter_day_map(df: pd.DataFrame):
        """Computing Quarter map column from date column,
        This function will modify the input dataframe

        Args:
            df (pd.DataFrame): timeseries dataframe needed to map with quarter data
        """
        df[["y", "m", "d"]] = df["day"].str.split("_", expand=True)
        df["Q"] = np.ceil(df["m"].astype(int) / 3)
        df["mapYQ"] = df["y"].astype(int) * 10 + df["Q"].astype(int)
        df["mapYQ"] = df.groupby('stock')["mapYQ"].shift(-1).ffill()
        df = df.drop(["y", "m", "d", "Q"], axis=1)

        return df
    
    @staticmethod
    def combine_conditions(conditions: list[pd.Series]):
        """Calculate condition from orther conditions"""
        res = None
        for cond in conditions:
            if cond is not None:
                if res is None:
                    res = cond
                else:
                    res = res & cond 
        return res


RUN = False
if __name__ == "__main__" and RUN:
    dft = Adapters.get_stock_from_vnstock("HPG")
    dft = Adapters.map_net_income(dft, "HPG")
