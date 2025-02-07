"""import libraries"""

import logging
import os
from datetime import datetime as dt
import pandas as pd
import numpy as np
from pymongo import MongoClient
from vnstock3 import Vnstock
from danglib.pylabview.src import Fns, sectors_paths
from redis import StrictRedis
import pickle

if "ACCEPT_TC" not in os.environ:
    os.environ["ACCEPT_TC"] = "tôi đồng ý"

HOST = "localhost"
r = StrictRedis()


class Adapters:
    """Adapter functions to get data from resources"""
    @staticmethod
    def get_index_data(start_day):
        # df: pd.DataFrame = pd.read_pickle("/home/ubuntu/Dang/data/index.pickle")
        # df = df.rename(columns={"value": "volume"})
        db = MongoClient('localhost',27022)['stockdata']
        index_coll = db['index_value']
        df: pd.DataFrame = pd.DataFrame(index_coll.find({},{'_id':0}))

        INDEX_LIST = ['VNINDEX','VN30','VNDIAMOND','VNMID','VNSML']

        df = df[(df['code'].isin(INDEX_LIST)) & (df['tradingDate'] >= start_day)].copy()
        df = df.drop_duplicates(subset = ['code','tradingDate'],keep='last')

        df = df[['code', 'tradingDate','open', 'close', 'high', 'low', 'totalMatchValue']].copy().rename(columns ={
            'code':'stock',
            'tradingDate':'day',
            'totalMatchValue':'volume'
        })
        df = df.sort_values(['stock', 'day'])
        return df
    
    @staticmethod
    def get_vnindex_from_db():
        db = MongoClient("localhost", 27022)["stockdata"]
        col = db['price_data']
        df_vnindex =pd.DataFrame(col.find({'stock':'VNINDEX','source':'VCI'},{'_id':0}))
        return df_vnindex

    @staticmethod
    def load_stocks_data_from_pickle():
        """Load stocks data from pickle file"""
        return pd.read_pickle(Fns.pickle_stocks_data)
    
    @staticmethod
    def load_stocks_data_from_main_db():
        """Load stocks data from main db"""
        try:
            db = MongoClient(HOST, 27022)['stockdata']
            col = db['pylabview_stocks_data']

            df = pd.DataFrame(list(col.find({}, {"_id":0})))
            return df
        except Exception as e:
            logging.error(f"Couldn't load data from main db: {e}")

        return pd.DataFrame()
    
    @staticmethod
    def load_stocks_data_from_plasma():
        from danglib.lazy_core import gen_plasma_functions
        _, disconnect, psave, pload = gen_plasma_functions(db=5)
        
        nummeric_data = pload("stocks_data_nummeric")
        stocks_i2s = pickle.loads(r.get("pylabview_stocks_i2s"))
        columns = pickle.loads(r.get("pylabview_stocks_data_columns"))
        
        df = pd.DataFrame(nummeric_data, columns=columns)
        df['stock'] = df['stock'].map(stocks_i2s)
        df['day'] = df['day'].astype(int).astype(str).apply(lambda x: f"{x[0:4]}_{x[4:6]}_{x[6:8]}")
        disconnect()
        
        return df
    

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
            ["ticker", "yearReport", "lengthReport", 'Attributable to parent company']
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
        db = MongoClient(HOST, 27022)["stockdata"]
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
        db = MongoClient(HOST, 27022)["stockdata"]
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
        df: pd.DataFrame = stock.quote.history(start="2014-01-01", end=dt.now().strftime(format="%Y-%m-%d"))
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
        db = MongoClient(HOST, 27022)["stockdata"]
        col = db["price_curated"]

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
        db = MongoClient(HOST, 27022)["stockdata"]
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
        db = MongoClient(HOST, 27022)["stockdata"]
        col_fiinpro = db['price_fiinpro_data']
        
        ['HoseStockId', 'OrganCode', 'Ticker', 'TradingDate', 
         'StockType', 'CeilingPrice', 'FloorPrice', 'ReferencePrice', 
         'ReferenceDate', 'OpenPrice', 'ClosePrice', 'MatchPrice', 
         'PriceChange', 'PercentPriceChange', 'HighestPrice', 
         'LowestPrice', 'AveragePrice', 'MatchVolume', 'MatchValue', 
         'DealVolume', 'DealValue', 'TotalMatchVolume', 'TotalMatchValue', 
         'TotalDealVolume', 'TotalDealValue', 'TotalVolume', 'TotalValue', 
         'ForeignBuyValueMatched', 'ForeignBuyVolumeMatched', 'ForeignSellValueMatched', 
         'ForeignSellVolumeMatched', 'ForeignBuyValueDeal', 'ForeignBuyVolumeDeal', 
         'ForeignSellValueDeal', 'ForeignSellVolumeDeal', 'ForeignBuyValueTotal', 
         'ForeignBuyVolumeTotal', 'ForeignSellValueTotal', 'ForeignSellVolumeTotal', 
         'ForeignTotalRoom', 'ForeignCurrentRoom', 'ParValue', 'Suspension',
         'Delist', 'HaltResumeFlag', 'Split', 'Benefit', 'Meeting', 'Notice', 
         'IssueDate', 'INav', 'IIndex', 'TotalTrade', 'TotalBuyTrade', 'TotalBuyTradeVolume', 
         'TotalSellTrade', 'TotalSellTradeVolume', 'ReferencePriceAdjusted', 'OpenPriceAdjusted', 
         'ClosePriceAdjusted', 'PriceChangeAdjusted', 'PercentPriceChangeAdjusted',
         'HighestPriceAdjusted', 'LowestPriceAdjusted', 'RateAdjusted', 'Status',
         'CreateDate', 'UpdateDate', 'Best1OfferPrice', 'Best2OfferPrice', 
         'Best3OfferPrice', 'Best1OfferVolume', 'Best2OfferVolume', 'Best3OfferVolume', 
         'Best1BidPrice', 'Best2BidPrice', 'Best3BidPrice', 'Best1BidVolume', 
         'Best2BidVolume', 'Best3BidVolume', 'CreateBy', 'UpdateBy', 'ShareIssue', 
         'TotalVolumeBuyUp', 'TotalVolumeSellDown', 'TotalMatchVolumeOdd', 
         'TotalMatchValueOdd', 'TotalDealVolumeOdd', 'TotalDealValueOdd', 
         'ForeignBuyValueMatchedOdd', 'ForeignBuyVolumeMatchedOdd', 'ForeignSellValueMatchedOdd', 
         'ForeignSellVolumeMatchedOdd', 'ForeignBuyValueDealOdd', 'ForeignBuyVolumeDealOdd', 
         'ForeignSellValueDealOdd', 'ForeignSellVolumeDealOdd', 'TotalBidCountOdd', 
         'TotalOfferCountOdd', 'TotalBidVolumeOdd', 'TotalOfferVolumeOdd', 
         'ForeignBuyVolumeMatchedEven', 'ForeignBuyValueMatchedEven', 'ForeignSellVolumeMatchedEven', 
         'ForeignSellValueMatchedEven', 'ForeignBuyVolumeDealEven', 'ForeignBuyValueDealEven', 
         'ForeignSellVolumeDealEven', 'ForeignSellValueDealEven', 'TotalBidCountEven', 
         'TotalOfferCountEven', 'TotalBidVolumeEven', 'TotalOfferVolumeEven']
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
        drop_stocks = ['ITA', 'LTG']
        path = Fns.stock_beta_group
        df = pd.read_excel(path, sheet_name="Sheet3")
        df = df[~df['Ticker'].isin(drop_stocks)]
        stocks = df["Ticker"].tolist()
        
        group_name_map = {1: "Super High Beta", 2: "High Beta", 3: "Medium", 4: "Low"}

        df["group_name"] = df["Group"].map(group_name_map)
        stocks_groups_map = dict(zip(df["Ticker"], df["group_name"]))
        return stocks, stocks_groups_map
    
    @staticmethod
    def load_quarter_netincome_from_db_VCI(symbols: list):
        db = MongoClient(HOST, 27022)["stockdata"]
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
                        "attributable_to_parent_company":1,
                        "revenue_bn_vnd":1
                    }
                )
            )
        )

        df = df.rename(columns = {
                'ticker':'stock',
                'yearreport': 'year',
                'lengthreport' : 'quarter',
                'revenue_bn_vnd' :'revenue',
                'attributable_to_parent_company' :'netIncome'
            }   
        )

        df['netIncome'] = df['netIncome'].fillna(0)
        df['revenue'] = df['revenue'].fillna(0)
        df["mapYQ"] = df["year"] * 10 + df["quarter"]
        df["mapYQ"] = np.where(df["quarter"] == 4, df["mapYQ"] + 7, df["mapYQ"] + 1)
        return df
    
    @staticmethod
    def load_fiinpro_PE_PB_ttm_daily(stocks, from_day="2016_01_01"):
        from_day = from_day.replace('_','-')
        db = MongoClient(HOST, 27022)["stockdata"]
        col = db['fiinpro_ratio_ttm_daily']
        
        df = pd.DataFrame(list(col.find(
                    {
                        'Ticker': {"$in":stocks},
                        "TradingDate": {"$gte":from_day},
                    },
                    {
                        "_id":0,
                        'Ticker':1,
                        'TradingDate':1,
                        "RTD21":1,
                        "RTD25":1
                    }
                )
            )
        )
        df.columns = ['stock', 'day', 'PE', 'PB']
        df['day'] = df['day'].str.replace('-', '_')
        return df
        
    @staticmethod
    def load_inventory_data_from_db(stocks, from_day="2016_01_01"):
        from_day = from_day.replace('_','-')
        db = MongoClient(HOST, 27022)["stockdata"]
        col = db['combined_price_fa']
        
        df = pd.DataFrame(list(col.find(
                    {
                        'stock': {"$in":stocks},
                        "day": {"$gte":from_day},
                    },
                    {
                        "_id":0,
                        'stock':1,
                        'day': 1,
                        'mapYQ':1,
                        "inventories_net_bn_vnd":1,
                    }
                )
            )
        )
        df.columns = ['stock','day', 'mapYQ', 'inventory']
        df['day'] = df['day'].str.replace('-', '_')
        df['inventory'] = df.groupby('stock')['inventory'].ffill()
        return df
    
    @staticmethod
    def load_balance_sheet_data(stocks, from_day="2016_01_01"):
        
        def test():
            stocks = ['HPG', 'SSI', 'VND', 'STB']
            from_day = '2016_01_01'
            
        db = MongoClient(HOST, 27022)['stockdata']
        
        # get YQ mapped data
        col1 = db['combined_price_fa']
        df = pd.DataFrame(
            list(
                col1.find(
                    {
                        'stock': {"$in":stocks},
                        "day": {"$gte":from_day},
                    },
                    {
                        "_id":0,
                        'stock':1,
                        'day': 1,
                        'mapYQ':1,
                        "inventories_net_bn_vnd":1,
                        "days_inventory_outstanding":1,
                        "purchase_of_fixed_assets":1,
                        "MarketCap":1
                    }
                )
            )
        )
        
        # df.columns = ['stock','day', 'mapYQ', 'inventory', 'inventoryDay', 'CAPEX', 'marketCap']
        df = df.rename(columns={
            'inventories_net_bn_vnd': 'inventory',
            'days_inventory_outstanding': 'inventoryDay',
            'purchase_of_fixed_assets': 'CAPEX', 
            'MarketCap': 'marketCap'
        })
        
        df['day'] = df['day'].str.replace('-', '_')
        df[
            ["marketCap","mapYQ","inventory","CAPEX","inventoryDay"]
        ] = df.groupby('stock')[
            ["marketCap","mapYQ","inventory","CAPEX","inventoryDay"]
        ].ffill()
        
        # Map YQ data:
        col2 =  db['VCI_balance_sheet_quarter']
        
        df2 = pd.DataFrame(
            list(
                col2.find(
                    {
                        'ticker': {"$in":stocks},
                    },
                    {
                        "_id":0,
                        'ticker':1,
                        'yearreport': 1,
                        'lengthreport':1,
                        "long_term_assets_bn_vnd":1,
                        "cash_and_cash_equivalents_bn_vnd":1,
                        "short_term_investments_bn_vnd":1,
                        "long_term_liabilities_bn_vnd":1,
                        "current_liabilities_bn_vnd":1,
                    }
                )
            )
        )
        df2 = df2.rename(columns={
            'ticker': 'stock',
            'yearreport': 'year', 
            'lengthreport': 'quarter',
            'long_term_assets_bn_vnd': 'gross', 
        })
        
        df2["mapYQ"] = df2["year"] * 10 + df2["quarter"]
        df2["mapYQ"] = np.where(df2["quarter"] == 4, df2["mapYQ"] + 7, df2["mapYQ"] + 1)
        
        df2 = df2.set_index("mapYQ")
        df2['cash'] = df2['cash_and_cash_equivalents_bn_vnd'] + df2['short_term_investments_bn_vnd']
        df2['debt'] = df2['long_term_liabilities_bn_vnd'] + df2['current_liabilities_bn_vnd']
        df2 = df2[['stock','gross', 'cash', 'debt']].copy()
        
        return df, df2
        
    
    @staticmethod
    def prepare_stocks_data(
            stocks,
            fn="stocks_data.pickle",
            db_collection=None,
            start_day='2017_01_01',
            to_pickle=False,
            to_mongo=True,
            to_plasma=False,
        ):
        def example_params():
            stocks = ['SSI', 'HPG']
            start_day = '2019_07_11'
        
        # df_stocks: pd.DataFrame = Adapters.get_stocks_data_from_db_fiinpro(stocks=stocks, from_day=start_day)
        
        db = MongoClient(HOST, 27022)["stockdata"]
        col = db['combined_price_fa']
        df_stocks = pd.DataFrame(list(col.find(
            {
                'day': {'$gte': start_day},
                'stock': {'$in': stocks}
            },
            {
                "_id": 0,
                'stock': 1,
                'day': 1,
                'open': 1,
                'close': 1,
                'high':1,
                'low':1,
                'value':1
            }
        )))
        
        
        if len(df_stocks['stock'].unique().tolist()) != len(stocks):
            missing_stocks = []
            for s in stocks:
                if s not in df_stocks['stock'].unique():
                    missing_stocks.append(s)
                    
            logging.warning(f"`prepare_stocks_data` error: Missing {missing_stocks} data in `combined_price_fa` db")

        df_stocks: pd.DataFrame = df_stocks.rename(columns={'value':'volume'})
        df_stocks = Utils.compute_quarter_day_map(df_stocks)
        
        # NetIncome, Revenue
        df_ni = Adapters.load_quarter_netincome_from_db_VCI(stocks)
        df_ni = df_ni.drop_duplicates()
        dfres = pd.merge(df_stocks, df_ni, how='left', on=['stock', 'mapYQ'])
        dfres = dfres.fillna(0)
        dfres = dfres.drop(['year', 'quarter'], axis=1)
        dfres = dfres.sort_values(['stock', 'day'])
        
        # P/E P/B
        df_pepb = Adapters.load_fiinpro_PE_PB_ttm_daily(stocks)
        dfres = pd.merge(dfres, df_pepb, how='left', on=['stock', 'day'])
        dfres[['PE', 'PB']] = dfres.groupby('stock')[['PE', 'PB']].ffill()
        
        # Margin Lending:
        df_margin = Adapters.Sectors.load_brokerage_margin_lending(stocks)
        dfres = pd.merge(dfres, df_margin, how='left', on=['stock', 'mapYQ'])
        
        # balance sheet:
        dfbs1, dfbs2 = Adapters.load_balance_sheet_data(stocks)
        dfbs2 = dfbs2.drop_duplicates()
        dfbs1 = dfbs1.drop('mapYQ', axis=1)
        dfres = pd.merge(dfres, dfbs1, how='left', on=['stock', 'day'])
        dfres[['marketCap','inventory','CAPEX','inventoryDay']] = dfres.groupby('stock')[['marketCap','inventory','CAPEX','inventoryDay']].ffill()
        
        dfres = pd.merge(dfres, dfbs2, how='left', on=['stock', 'mapYQ'])
        
        df_indexs = Adapters.get_index_data(start_day=start_day)
        dfres = pd.concat([dfres, df_indexs]).reset_index(drop=True)
        
        stocks_i2s = {i: s for i , s in enumerate(dfres['stock'].unique())}
        stocks_s2i = {s: i for i, s in stocks_i2s.items()}
        columns = dfres.columns.tolist()
        dfres = dfres.drop_duplicates()
        r.set("pylabview_stocks_i2s", pickle.dumps(stocks_i2s))
        r.set("pylabview_stocks_data_columns", pickle.dumps(columns))
        
        
        def df_stocks_to_num(df):
            df = dfres.copy()
            df['stock'] = df['stock'].map(stocks_s2i)
            df['day'] = df['day'].astype(int)
            df.to_numpy()
            return df.to_numpy()
            
        
        if to_pickle:
            dfres.to_pickle(fn)
        if to_mongo:
            if db_collection is None:
                db = MongoClient(HOST, 27022)['stockdata']
                db_collection = db['pylabview_stocks_data']
            db_collection.drop()
            db_collection.insert_many(dfres.to_dict("records"))
        if to_plasma:
            from danglib.lazy_core import gen_plasma_functions

            _, disconnect, psave, pload = gen_plasma_functions(db=5)
            
            dfp = df_stocks_to_num(dfres)
            psave("stocks_data_nummeric", dfp)
            disconnect()
            
    
    
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
            
            res['brokerage'] = {
                'categories': pd.DataFrame({
                    'name': ['deposit rate'],
                    'categories': ["Input"],
                    'available': [True]
                }),
                'io_data': Adapters.Sectors.load_brokerage_bank_rates()
            }
            res['fish'] = Adapters.Sectors.load_fish_data()
            res['hog'] = Adapters.Sectors.load_hog_data()
            res['fertilizer'] = Adapters.Sectors.load_fertilizer()

        return res
    
    class Sectors:
        @staticmethod
        def load_brokerage_margin_lending(stocks:list):
            df: pd.DataFrame = pd.read_excel(**Fns.sectors_rawdata['brokerage']['margin_lending'])
            df = df.rename(columns={'Unnamed: 0':'stock'})
            df = df[df['stock'].isin(stocks)]
            df = df.set_index('stock').transpose()
            df['year'] = df.index.str[-2:].astype(int)+2000
            df['quarter'] = df.index.str[:1].astype(int)
            
            df['mapYQ'] = df['year'] * 10 + df['quarter']
            df["mapYQ"] = np.where(df["quarter"] == 4, df["mapYQ"] + 7, df["mapYQ"] + 1)
            df = df.drop(['quarter', 'year'], axis=1)
            df = df.set_index('mapYQ')
            df = df.stack().reset_index(name='marginLending')
            return df
        
        @staticmethod
        def load_brokerage_bank_rates():
            df: pd.DataFrame = pd.read_excel(**Fns.sectors_rawdata['brokerage']['bank_rates'])
            df = df[['Date', '12M']].copy()
            df['Date'] = df['Date'].astype(str).str[:10]
            df['Date'] = df['Date'].str.replace("-", "_")
            df.columns = ['day', 'depositRate']
            return df
        
        @staticmethod
        def load_fish_data():
            df: pd.DataFrame = pd.read_excel(**Fns.sectors_rawdata['fish']['fish_data'])
            df = df.drop(0)
            df.columns = ['day', 'vhcUsAsp', 'rawFishPrice', 'usPriceSpread']
            df['day'] = df['day'].astype(str)
            df['day'] = df['day'].str[:10]
            df['day'] = df['day'].str.replace('-', '_')
            
            df_cate = pd.DataFrame({
                'name':[i for i in df.columns if i != 'day']
            })
            df_cate['categories'] = 'Input'
            df_cate['available'] = True
            return {
                'categories': df_cate,
                'io_data': df
            }

        @staticmethod
        def load_hog_data():
            df: pd.DataFrame = pd.read_excel(**Fns.sectors_rawdata['hog']['hog_data'], header=3)
            df =  df[['Date','Average Northern live hog price', 'Average Southern live hog price', 'Feed cost per kg of live hog', 'Cash spread (ASP - feed cost)']]
            df = df.dropna(how='all')
            df.columns = ['day', 'avgNorthHogPrice', 'avgSouthHogPrice', 'hogFeedCost', 'hogCashSpread']
            df['day'] = df['day'].astype(str)
            df['day'] = df['day'].str[:10]
            df['day'] = df['day'].str.replace('-', '_')
            
            df_cate = pd.DataFrame({
                'name':[i for i in df.columns if i != 'day']
            })
            df_cate['categories'] = 'Input'
            df_cate['available'] = True
            return {
                'categories': df_cate,
                'io_data': df
            }


        @staticmethod
        def load_fertilizer():
            def load_china_p4():
                df: pd.DataFrame = pd.read_excel(**Fns.sectors_rawdata['fertilizer']['p4_price'], skiprows=5)
                df = df[['Date', 'China P4 price', 'China P4 cash spread']]
                df.columns = ['day', 'ChinaP4Price', 'ChinaP4CashSpread']
                
                return df
            
            def load_urea_data():
                df_dcm: pd.DataFrame = pd.read_excel(**Fns.sectors_rawdata['fertilizer']['DCM_urea'])
                df_future: pd.DataFrame = pd.read_excel(**Fns.sectors_rawdata['fertilizer']['urea_future'])
                df_dpm: pd.DateOffset = pd.read_excel(**Fns.sectors_rawdata['fertilizer']['DPM_urea'])

                df_future = df_future[df_future['Date'] >= '2018-01-01']
                df = pd.merge(df_future, df_dcm, how='outer', on='Date')
                df = pd.merge(df, df_dpm, how='outer', on='Date')
                df.columns = ['day', 'future', 'DCM', 'DPM']
                return df
            
            df_p4 = load_china_p4()
            df_urea = load_urea_data()
            
            df = pd.merge(df_p4, df_urea, how='outer', on='day')

                
            df['day'] = df['day'].astype(str)
            df['day'] = df['day'].str[:10]
            df['day'] = df['day'].str.replace('-', '_')
            df = Utils.remove_saturday_sunday(df, day_col='day')                   
            df_cate = pd.DataFrame({
                'name':[i for i in df.columns if i != 'day']
            })
            df_cate['categories'] = 'Input'
            df_cate['available'] = True
            
            return {
                'categories': df_cate,
                'io_data': df
            }


class Ta:
    """Technical analysis"""
    
    @staticmethod
    def rolling_rank(src: pd.Series, ranking_window):
        src_rank = (src.rolling(ranking_window).rank() - 1) / (ranking_window - 1) * 100
        return src_rank

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

        def test():
            src_name: str = "close"
            bb_length: int = 20
            length_kc: int = 20
            mult_kc: float = 1.5
            use_true_range: bool = True
            df = glob_obj.get_one_stock_data("ABB")

        p_h = df["high"]
        p_l = df["low"]
        p_c = df["close"]
        src = df[src_name]

        basis = Ta.sma(src, bb_length)
        dev = mult_kc * Ta.stdev(src, bb_length)
        upper_bb = basis + dev
        lower_bb = basis - dev

        sqz_ma = Ta.sma(src, length_kc)

        sqz_range = (
            Math.max([p_h - p_l, abs(p_h - p_c.shift(1)), abs(p_l - p_c.shift(1))], skipna=False)
            if use_true_range
            else (p_h - p_l)
        )

        rangema = Ta.sma(sqz_range, length_kc)
        upper_kc = sqz_ma + rangema * mult_kc
        lower_kc = sqz_ma - rangema * mult_kc

        sqz_on = (lower_bb > lower_kc) & (upper_bb < upper_kc)
        sqz_off = (lower_bb < lower_kc) & (upper_bb > upper_kc)
        no_sqz = pd.Series(np.where(sqz_on | sqz_off, False, True))

        def test2():
            dft = df['day'].to_frame('day')
            dft['sqz_on'] = sqz_on
            dft['sqz_ma'] = sqz_ma
            dft['sqz_range'] = sqz_range
            dft['rangema'] = rangema
            
            dft['lower_bb'] = lower_bb
            dft['lower_kc'] = lower_kc
            dft['upper_bb'] = upper_bb
            dft['upper_kc'] = upper_kc
            dft = dft.set_index('day')
            dft.loc['2021_01_25']

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
        df: pd.DataFrame, src_name: str = "close", length: int = 20, mult: float = 2, return_upper_lower = False
    ):
        """bbpctb"""
        src = df[src_name].copy()
        basic = Ta.sma(src, length)
        dev = mult * Ta.stdev(src, length)
        upper = basic + dev
        lower = basic - dev
        bbpctb = (src - lower) / (upper - lower) * 100
        if not return_upper_lower:
            return bbpctb
        else:
            return bbpctb, upper, lower
    @staticmethod
    def wavetrend(
        df: pd.DataFrame, n1 = 10, n2 = 21
    ):
        def test():
            # df = df_raw.copy()
            n1 = 10
            n2 = 21
        p_high = df['high']
        p_low = df['low']
        p_close = df['close']
        hlc3 = (p_high + p_low + p_close)/3

        wt_ap = hlc3
        wt_esa = Ta.ma(wt_ap, n1, 'EMA')
        wt_d = Ta.ma(abs(wt_ap - wt_esa), n1, 'EMA')
        wt_ci = (wt_ap - wt_esa) / (0.015 * wt_d)
        wt_tci = Ta.ma(wt_ci, n2, 'EMA')

        wt1 = wt_tci
        wt2 = Ta.ma(wt1, 4, 'SMA')

        return wt1, wt2

    @staticmethod
    def fourier_supertrend(
        df: pd.DataFrame,
        # src_name: str = 'close',
        fft_period: int = 14,
        fft_smooth: int = 7,
        harmonic_weight: float = 0.5,
        vol_length: int = 10,
        vol_mult: float = 2.0,
        vol_smooth: int = 10
    ):
        def get_fft_trend(src, length, harmonic_weight):
           
            wave1 = src.ewm(span=length, adjust=False).mean()
            wave2 = src.ewm(span=length // 2, adjust=False).mean()
            wave3 = src.ewm(span=length // 3, adjust=False).mean()

            trend = wave1 + harmonic_weight * (wave2 - wave1) + harmonic_weight**2 * (wave3 - wave2)
            return trend

        def get_adaptive_volatility(df, vol_length):
            # atr = AverageTrueRange(high=df['high'], low=df['low'], close=df['close'], window=vol_length).average_true_range()
            tr = Math.max([df['high'] - df['low'], abs(df['high'] - df['close'].shift(1)), abs(df['low'] - df['close'].shift(1))], skipna=False)
            atr = Ta.sma(tr, vol_length)
            std = df['close'].rolling(window=vol_length).std(ddof=0)
            vol = (atr + std) / 2
            return vol

        # Tính Fourier Trend
        fft_trend = get_fft_trend(df['close'], fft_period, harmonic_weight)
        fft_trend = fft_trend.ewm(span=fft_smooth, adjust=False).mean()

        # Tính volatility
        volatility = get_adaptive_volatility(df, vol_length=vol_length)
        volatility = volatility.ewm(span=vol_smooth, adjust=False).mean()

        # Dải Supertrend
        upper_band = fft_trend + volatility * vol_mult
        lower_band = fft_trend - volatility * vol_mult

        return fft_trend, upper_band, lower_band    
    
class VTa:
    """TA functions used for vector calculations"""

    @staticmethod
    def min(src1:pd.DataFrame, src2:pd.DataFrame):
        return pd.DataFrame(np.minimum(src1.values, src2.values), columns=src1.columns, index=src1.index)

    @staticmethod
    def max(src1:pd.DataFrame, src2:pd.DataFrame):
        return pd.DataFrame(np.maximum(src1.values, src2.values), columns=src1.columns, index=src1.index)


class Math:
    """mathematical functions for pandas Series"""

    @staticmethod
    def max(series_ls: list[pd.Series], skipna=True):
        """To calculate the maximum value for each row from multiple Pandas Series.

        Args:
            series_ls (list): A sequence of series to use in the calculation.

        Returns:
            pd.Series: The series of maximum values for each row of the input series
        """
        return pd.concat(series_ls, axis=1).max(axis=1, skipna=skipna)

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
        df = df.sort_values(['stock', 'day'])
        df["mapYQ"] = df.groupby('stock')["mapYQ"].shift(-1).ffill()
        df = df.drop(["y", "m", "d", "Q"], axis=1)
        df = df.reset_index(drop=True)
        return df
    
    @staticmethod
    def compute_quarter_lastday_map(df: pd.DataFrame):
        """From dataframe stocks data, extract net Income map with last quarter date for each stock"""
        df[df.isna().any(axis=1)]
        df['dif'] = df['mapYQ'].diff()
        t = df[(df['dif'] != 0) & (~df['dif'].isna())].copy()
        t['netIncome'] = t['netIncome'].fillna(0)
        t[t.isna().any(axis=1)]
        dfres = t[['day', 'stock', 'netIncome']].copy()
        return dfres
    
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
    
    @staticmethod
    def merge_condition(df: pd.DataFrame, day_mapped_cond: pd.Series, fillna=True):
        if day_mapped_cond is not None:
            df['cond'] = df['day'].map(day_mapped_cond)
            if fillna:
                df['cond'] = df['cond'].fillna(False)
            return df["cond"]

    @staticmethod
    def compare_two_dataframe(df1: pd.DataFrame, df2: pd.DataFrame, subset=None, sort_values=None):
        df1 = df1.copy()
        df2 = df2.copy()
        if subset is None:
            subset = df1.columns.tolist()

        df1['df_version'] = 1
        df2['df_version'] = 2


        dfc = pd.concat([df1, df2], ignore_index=True)
        df_diff = dfc.drop_duplicates(subset=subset, keep=False)

        if sort_values is not None:
            df_diff = df_diff.sort_values(by=sort_values)

        return df_diff
    
    @staticmethod
    def remove_saturday_sunday(df: pd.DataFrame, day_col='day'):

        # Convert the date column to datetime
        df['date'] = pd.to_datetime(df[day_col], format='%Y_%m_%d')

        # Create a new column to indicate if the date is a weekend
        df['is_weekend'] = df['date'].dt.weekday.isin([5, 6])

        df = df[~df['is_weekend']].copy()
        df = df.drop(['date', 'is_weekend'], axis=1)

        return df


RUN = False
if __name__ == "__main__" and RUN:
    dft = Adapters.get_stock_from_vnstock("HPG")
    dft = Adapters.map_net_income(dft, "HPG")
