from danglib.lazy_core import gen_plasma_functions
from danglib.pslab.resources import Globs
import pandas as pd
from redis import StrictRedis
from datetime import datetime
from danglib.utils import day_to_timestamp

def load_realtime_data():
    _, disconnect, _, pload = gen_plasma_functions(Globs.PLASMA_DB)
    key = "pslab_realtime_stockdata2.30S"
    df = pload(key)
    return df

today = datetime.now().strftime('%Y_%m_%d')
day_stamp = day_to_timestamp(today) // 1e9

from danglib.pslab.process_data3 import Resampler, ProcessStockData
processData = ProcessStockData
df = processData.load_realtime_data(
    r = StrictRedis(decode_responses=True),
    day=today,
    start=0,
    end=-1
)
df = processData.preprocess_realtime_data(df, day=today)
df30 = Resampler.transform(
    day=today,
    df=df,
    timeframe='30S',
    agg_dic=processData.get_agg_dic(),
    cleanup_data=True,
    subgroup='stock'
)
df30 = processData.postprocess_resampled_data(df30)
df30.columns.get_level_values(0).unique()
df30

dfr = load_realtime_data()
dfr = dfr[dfr.index > day_stamp]

stock = "HPG"
dfrs = dfr.xs(stock, level=1, axis=1)
df30s = df30.xs(stock, level=1, axis=1)

import plotly.express as px

def to_date(timestamp):
    return pd.to_datetime(timestamp, unit='s')
stats = 'matchingValue'
fig = px.line(dfrs, x=to_date(dfrs.index), y=stats)
fig.add_scatter(x=to_date(df30s.index), y=df30s[stats], mode='lines', name='df30s')
fig.show()

#%%
# Resample history data using new ProcessStockData
import pandas as pd
from danglib.pslab.process_data3 import ProcessStockData, Resampler
day= '2025_02_28'
def hose500_history_adapter():
    df = pd.read_pickle("/data/dang/test_hose500_data.pickle")
    df = df.rename(columns={
        'last': 'close',
    })
    return df
df = hose500_history_adapter()

df = ProcessStockData.preprocess_realtime_data(df, day=day)
dfnew = Resampler.transform(
    day=day,
    df=df,
    timeframe='30S',
    agg_dic=ProcessStockData.get_agg_dic(),
    cleanup_data=True,
    subgroup='stock'
)
dfnew = ProcessStockData.postprocess_resampled_data(dfnew)
#%%
# Load Resampled history data:
import pandas as pd
from danglib.pslab.process_data3 import ProcessStockData
dfhis = ProcessStockData.load_history_data(from_day=day)