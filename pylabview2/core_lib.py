"""import libraries"""

import logging
import os
from datetime import datetime as dt
import pandas as pd
import numpy as np
from pymongo import MongoClient
from vnstock3 import Vnstock
from numba import njit
from danglib.pylabview.src import Fns, sectors_paths

if "ACCEPT_TC" not in os.environ:
    os.environ["ACCEPT_TC"] = "tôi đồng ý"

today = dt.now().strftime(format="%Y-%m-%d")

class Adapters:
    """Adapter functions to get data from resources"""

    @staticmethod
    def load_stocks_data_from_pickle():
        """Load stocks data from pickle file"""
        return pd.read_pickle(Fns.pickle_stocks_data)
    
    @staticmethod
    def load_stocks_data_from_main_db():
        """Load stocks data from main db"""
        try:
            db = MongoClient('ws', 27022)['stockdata']
            col = db['pylabview_stocks_data']

            df = pd.DataFrame(list(col.find({}, {"_id":0})))
            return df
        except Exception as e:
            logging.error(f"Couldn't load data from main db: {e}")

        return pd.DataFrame()
        

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
    def get_stocks_data_from_db_fiinpro(stocks: list, from_day: str = '2017_01_01'):
        db = MongoClient("ws", 27022)["stockdata"]
        col_fiinpro = db['price_fiinpro_data']

        df = pd.DataFrame(
            list(
                col_fiinpro.find(
                    {"Ticker": {"$in": stocks}, "TradingDate": {"$gte": from_day.replace('_','-')}}, 
                    {
                        "_id":0,
                        'TradingDate':1,
                        'Ticker':1,
                        'OpenPriceAdjusted' : 1,
                        'ClosePriceAdjusted' : 1,
                        'HighestPriceAdjusted' : 1,
                        'LowestPriceAdjusted' : 1,
                        'TotalMatchValue':1,
                    }
                )
            )
        )

        df = df.rename(
            columns = { 
                'TradingDate':'day',
                'Ticker' : 'stock', 
                'OpenPriceAdjusted' : 'open',
                'ClosePriceAdjusted' : 'close',
                'HighestPriceAdjusted' : 'high',
                'LowestPriceAdjusted' : 'low',
                'TotalMatchValue':'volume',
            }
        )
    
        df['day'] = df['day'].str.replace('-','_')
        df = df[df['volume']>0].copy()
        
        return df

    @staticmethod
    def get_stocks_beta_group():
        """Get stocks list and group by Beta from excel file

        Returns:
            stocks (list): stocks list,
            stocks_groups_map (dict): map stock with beta group
        """
        path = Fns.stock_beta_group
        df = pd.read_excel(path, sheet_name="Sheet3")
        stocks = df["Ticker"].tolist()

        group_name_map = {1: "Super High Beta", 2: "High Beta", 3: "Medium", 4: "Low"}

        df["group_name"] = df["Group"].map(group_name_map)
        stocks_groups_map = dict(zip(df["Ticker"], df["group_name"]))
        return stocks, stocks_groups_map
    
    @staticmethod
    def load_quarter_netincome_from_db_VCI(symbols: list):
        db = MongoClient("ws", 27022)["stockdata"]
        col = db['VCI_income_statement_quarter']
        df = pd.DataFrame(
            list(
                col.find(
                    {'ticker':{"$in":symbols}},
                    {
                        "_id":0, 
                        "ticker":1,
                        "yearreport":1,
                        "lengthreport":1,
                        "net_profitloss_before_tax":1
                    }
                )
            )
        )
        df.columns = ["stock", "year", "quarter", "netIncome"]
        df['netIncome'] = df['netIncome'].fillna(0)
        df["mapYQ"] = df["year"] * 10 + df["quarter"]
        df["mapYQ"] = np.where(df["quarter"] == 4, df["mapYQ"] + 7, df["mapYQ"] + 1)
        return df
    

    @staticmethod
    def prepare_stocks_data(stocks, fn="stocks_data.pickle", db_collection=None, start_day='2017_01_01', to_pickle=False, to_mongo=True):
        df_stocks: pd.DataFrame = Adapters.get_stocks_data_from_db_fiinpro(stocks=stocks, from_day=start_day)
        df_stocks = Utils.compute_quarter_day_map(df_stocks)
        
        df_ni = Adapters.load_quarter_netincome_from_db_VCI(stocks)
        dfres = pd.merge(df_stocks, df_ni, how='left', on=['stock', 'mapYQ'])
        dfres['netIncome'] = dfres['netIncome'].fillna(0)
        dfres = dfres.drop(['year', 'quarter'], axis=1)
        dfres = dfres.sort_values(['stock', 'day'])
        
        if to_pickle:
            dfres.to_pickle(fn)
        if to_mongo:
            if db_collection is None:
                db = MongoClient('ws', 27022)['stockdata']
                db_collection = db['pylabview_stocks_data']
            db_collection.drop()
            db_collection.insert_many(dfres.to_dict("records"))

        return dfres

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

    @staticmethod
    def load_sectors_data():

        def load_excel_sheet(path, sheet_name, header=0):
            df: pd.DataFrame = pd.read_excel(path, sheet_name=sheet_name, header=header)
            return df
        
        def load_io_data(path, sheet_name, header=0):
            df: pd.DataFrame = load_excel_sheet(path, sheet_name, header)
            df = df.rename(columns={'Dates': 'day'})
            df['day'] = df['day'].astype(str).str.replace("-", "_")
            df = df.sort_values('day')
            return df
        

        file_info_cached = {}
        res = {}

        for name, data in sectors_paths.items():
            cate_info = data['categories']
            io_data = data['io_data']

            cate_cached_name = cate_info['path'] + cate_info.get('sheet_name', 'Sheet1')
            if cate_cached_name not in file_info_cached:
                file_info_cached[cate_cached_name] = load_excel_sheet(**cate_info)

            df_cate: pd.DataFrame = file_info_cached[cate_cached_name]

            io_cached_name = io_data['path'] + io_data.get('sheet_name', 'Sheet1')
            if io_cached_name not in file_info_cached:
                file_info_cached[io_cached_name] = load_io_data(**io_data)

            df_io: pd.DataFrame = file_info_cached[io_cached_name]

            df_cate['available'] = df_cate['Name'].isin(df_io.columns)
            df_cate.columns = ['name', 'categories', 'available']

            res[name] = {
                'categories': df_cate,
                'io_data': df_io
            }

        return res

class Ta:
    """TA functions used for vector calculations"""

    @staticmethod
    def min(src1:pd.DataFrame, src2:pd.DataFrame):
        return pd.DataFrame(np.minimum(src1.values, src2.values), columns=src1.columns, index=src1.index)

    @staticmethod
    def max(src1:pd.DataFrame, src2:pd.DataFrame):
        return pd.DataFrame(np.maximum(src1.values, src2.values), columns=src1.columns, index=src1.index)

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
    def ma(src: pd.Series, length, ma_type):
        """
        if ma_type == "EMA":
            return Ta.ema(src, length)
        if ma_type == "SMA":
            return Ta.sma(src, length)
        if ma_type == "RMA":
            return Ta.rma(src, length)
        """

        if ma_type == "EMA":
            return Ta.ema(src, length)
        if ma_type == "SMA":
            return Ta.sma(src, length)
        return Ta.sma(src, length)
    
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
    def is_lowest(src: pd.Series, length: int):
        """Check if is lowest value for a given period.

        Args:
            src (pd.Series): Series of values to process.
            length (_type_): Number of bars.

        Returns:
            pd.Series: if is lowest value for a given period
        """
        return src.rolling(length).min() == src
    
class Utils:
    """Ultilities"""
    
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
