from redis import StrictRedis
import pandas as pd
import json




r = StrictRedis()

# key = "pylabview_hose_2025_02_10"  

# df = pd.DataFrame(json.loads(x) for x in r.lrange(key, 0, -1))


class Adapters:
    @staticmethod
    def load_realtime_stock_data_from_redis(r: StrictRedis, day, start, end):
        key = f"pylabview_hose_{day}"
        df = pd.DataFrame(json.loads(x) for x in r.lrange(key, start, end))
        return df
    

class Preprocessor:

    @staticmethod
    def preprocess_realtime_stock_data(df: pd.DataFrame):

        def test():
            df = Adapters.load_realtime_stock_data_from_redis(r, "2025_02_11", 0, -1)

        


class Resampler:
    AGG_DICT = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }

    def __init__(self, df: pd.DataFrame, timeframe, agg_dict=None, preprocess_func=None) -> None:
        self.df = df if preprocess_func is None else preprocess_func(df)
        self.timeframe = timeframe
        self.agg_dict = agg_dict if agg_dict else self.AGG_DICT
        self.preprocess_func = preprocess_func

    def resample(self):
        pass