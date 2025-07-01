from danglib.pslab.funcs import CombiConds, Globs


def create_strategy(name, conditions, group, direction, ftype, holding_periods=60):
    return {
        'name': name,
        'conditions': conditions,
        'group': group,
        'type': direction,
        'ftype': ftype,
        'holding_periods': holding_periods
    }


conditions = [
    create_strategy('strategy1', [
        {
            'function': 'two_line_pos',
            'inputs': {
                'src1': 'bu',
                'src2': 'sd',
                'stocks': Globs.SECTOR_DIC['VN30'],
            },
            'params': {
                'direction': 'crossover'
            }
        },
    ], 1, 'long', 'single'),
    create_strategy('strategy2', [
        {
            'function': 'two_line_pos',
            'inputs': {
                'src1': 'bu2',
                'src2': 'sd2',
                'stocks': Globs.SECTOR_DIC['VN30'],
            },
            'params': {
                'direction': 'crossover'
            }
        },
    ], 1, 'short', 'single'),
    create_strategy('strategy3', [
        {
            'function': 'two_line_pos',
            'inputs': {
                'src1': 'bu',
                'src2': 'sd',
                'rolling_window': 20,
                'stocks': Globs.SECTOR_DIC['VN30'],
            },
            'params': {
                'direction': 'crossunder'
            }
        }
    ], 1, 'long', 'single'),
    create_strategy('strategy4', [
        {
            'function': 'two_line_pos',
            'inputs': {
                'src1': 'bu2',
                'src2': 'sd2',
                'rolling_window': 20,
                'stocks': Globs.SECTOR_DIC['Super High Beta'],
            },
            'params': {
                'direction': 'crossover'
            }
        }
    ], 1, 'long', 'single'),
    create_strategy('strategy5', [
        {
            'function': 'two_line_pos',
            'inputs': {
                'src1': 'bid',
                'src2': 'ask',
                'rolling_window': 20,
                'rolling_method': 'median',
                'stocks': Globs.SECTOR_DIC['VN30'],
            },
            'params': {
                'direction': 'crossunder'
            }
        }
    ], 2, 'short', 'single'),
    create_strategy('strategy6', [
        {
            'function': 'two_line_pos',
            'inputs': {
                'src1': 'bid',
                'src2': 'ask',
                'rolling_window': 20,
                'rolling_method': 'median',
                'stocks': Globs.SECTOR_DIC['VN30'],
            },
            'params': {
                'direction': 'crossover'
            }
        }
    ], 2, 'short', 'single'),
    create_strategy('strategy7', [
        {
            'function': 'two_line_pos',
            'inputs': {
                'src1': 'bid',
                'src2': 'ask',
                'rolling_window': 20,
                'rolling_method': 'median',
                'stocks': Globs.SECTOR_DIC['VN30'],
            },
            'params': {
                'direction': 'crossunder'
            }
        }
    ], 2, 'short', 'single'),
    create_strategy('strategy8', [
        {
            'function': 'two_line_pos',
            'inputs': {
                'src1': 'bid',
                'src2': 'ask',
                'rolling_window': 20,
                'rolling_method': 'median',
                'stocks': Globs.SECTOR_DIC['VN30'],
            },
            'params': {
                'direction': 'crossover'
            }
        }
    ], 2, 'short', 'single'),
    create_strategy('strategy9', [
        {
            'function': 'two_line_pos',
            'inputs': {
                'src1': 'bid',
                'src2': 'ask',
                'rolling_window': 20,
                'rolling_method': 'median',
                'stocks': Globs.SECTOR_DIC['VN30'],
            },
            'params': {
                'direction': 'crossunder'
            }
        }
    ], 3, 'short', 'combo2'),
    create_strategy('strategy10', [
        {
            'function': 'two_line_pos',
            'inputs': {
                'src1': 'bid',
                'src2': 'ask',
                'rolling_window': 20,
                'rolling_method': 'median',
                'stocks': Globs.SECTOR_DIC['VN30'],
            },
            'params': {
                'direction': 'above'
            }
        }
    ], 3, 'short', 'combo2'),
    create_strategy('strategy11', [
        {
            'function': 'two_line_pos',
            'inputs': {
                'src1': 'bid',
                'src2': 'ask',
                'rolling_window': 20,
                'rolling_method': 'median',
                'stocks': Globs.SECTOR_DIC['VN30'],
            },
            'params': {
                'direction': 'below'
            }
        }
    ], 3, 'short', 'combo2'),
]


import json
with open('/home/ubuntu/Dang/pslab_strategies.json', 'w') as f:
    json.dump(conditions, f)

def group_conditions(conditions_params: dict):
    """
        Process strategies that use stock data.
        
        Args:
            conditions_params: List of strategies with stocks
            
        Returns:
            pd.Series of boolean signals
    """
    def test():
        conditions_params = [
            {
                'function': 'two_line_pos',
                'inputs': {
                    'src1': 'bu',
                    'src2': 'sd',
                    'stocks': Globs.SECTOR_DIC['VN30'],
                },
                'params': {
                    'direction': 'crossover'
                }
            },
        ]

    
    if not conditions_params:
        return None
        
    # Load stock data
    required_data, updated_params = CombiConds.load_and_process_group_data2(conditions_params, realtime=True)
    
    # Generate signals
    signals = CombiConds.combine_conditions(required_data, updated_params)

    return signals

def compute_all_signals():

    from danglib.pslab.pslab_worker import compute_signals, clean_redis
    from tqdm import tqdm 
    import time

    class CeleryTaskError(Exception):
        pass

    task_ls = []
    active_tasks = set()

    with tqdm(total=len(conditions),
            desc=f"Submitting and processing tasks") as pbar:
        conditions_iter = iter(conditions)
        submitted_count = 0

        while submitted_count < len(conditions):
            active_tasks = {task for task in active_tasks 
                        if task.status not in ['SUCCESS', 'FAILURE']}
            
            while len(active_tasks) < 5 and submitted_count < len(conditions):
                try:
                    condition = next(conditions_iter)
                    task = compute_signals.delay(
                        strategy=condition
                    )
                    task_ls.append(task)
                    active_tasks.add(task)
                    submitted_count += 1
                    pbar.update(1)
                except StopIteration:
                    break
            
            time.sleep(0.1)

    with tqdm(total=len(task_ls),
            desc=f"Processing tasks") as pbar:
        completed_tasks = set()
        while len(completed_tasks) < len(task_ls):
            for i, task in enumerate(task_ls):
                if i not in completed_tasks and task.status in ['SUCCESS', 'FAILURE']:
                    if task.status == 'FAILURE':
                        raise CeleryTaskError(f"Task failed: {task.id}")
                    completed_tasks.add(i)
                    pbar.update(1)
            time.sleep(0.1)

    chunk_results = []
    with tqdm(total=len(task_ls), desc=f"Collecting results") as pbar:
        for task in task_ls:
            try:
                result = task.result
                if result is not None:
                    chunk_results.append(result)
            except Exception as e:
                print(f"Error collecting results for strategy {task}: {str(e)}")
            pbar.update(1)

    import pandas as pd

    df = pd.concat(chunk_results)

    from danglib.lazy_core import gen_plasma_functions
    _, disconnect, psave, pload = gen_plasma_functions(db=Globs.PLASMA_DB)
    psave('pslab_strategies_realtime_signals', df)
                
    clean_redis()


    return df

# from danglib.pslab.pslab_worker import calculate_next_candletime

# import time
# from datetime import datetime

# while True:
#     try:
#         compute_all_signals()

#         current_stamp = time.time() + 7*3600
#         next_candle_time = calculate_next_candletime(current_stamp, '30S')
#         sleep_duration = next_candle_time - current_stamp

        
#         if sleep_duration > 0:
#             time.sleep(sleep_duration)


#     except Exception as e:
#         print(e)
#         time.sleep(1)

#%% test plasma

from danglib.lazy_core import gen_plasma_functions
_, disconnect, psave, pload = gen_plasma_functions(db=Globs.PLASMA_DB)
from danglib.utils import day_to_timestamp
import pandas as pd

df = pload('pslab_realtime_psdata2.30S')
day = '2025_03_03'
day_stamp = day_to_timestamp(day) // 1e9
df = df[df.index >= day_stamp]

df['candle'] = pd.to_datetime(df.index, unit='s')

import plotly.graph_objects as go

fig = go.Figure(data=[go.Candlestick(x=df['candle'],
                open=df['F1Open'],
                high=df['F1High'],
                low=df['F1Low'],
                close=df['F1Close'])])

# Update layout
fig.update_layout(
    title='Candlestick Chart',
    yaxis_title='Price',
    xaxis_title='Time',
    template='plotly_dark'  # Using dark theme
)

# Show the plot
fig.show()

from danglib.pslab.resources import Adapters
from redis import StrictRedis
r = StrictRedis(host='localhost', decode_responses=True)
df =Adapters.RedisAdapters.load_realtime_PS_data_from_redis(r, '2025_03_04')

import plotly.express as px
df = df.sort_values('timestamp')
df['candle'] = pd.to_datetime(df['timestamp'], unit='ms')
df = df[df['candle'] > '2025-03-03 02:15:00']
df = df.resample('30S', on='candle').agg({
    'lastPrice': 'ohlc',
})
df = df.droplevel(0, axis=1).reset_index()

import plotly.graph_objects as go

fig = go.Figure(data=[go.Candlestick(x=df['candle'],
                open=df['open'],
                high=df['high'],
                low=df['low'],
                close=df['close'])])

# Update layout
fig.update_layout(
    title='Candlestick Chart',
    yaxis_title='Price',
    xaxis_title='Time',
    template='plotly_dark'  # Using dark theme
)

# Show the plot
fig.show()

#%%

