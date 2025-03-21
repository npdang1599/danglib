from danglib.pslab.resources import Globs
true = True
false = False

class TestSets:
    LOAD_PROCESS_GROUP_DATA = [
        {
            "function": "MA",
            "inputs": {
                "src": "fBuyVal",
                "rolling_window": 1,
                "rolling_method": "sum",
                "daily_rolling": true,
                "exclude_atc": false,
                "scale": 1,
                "timeframe": "30S",
                "stocks": [
                    "ACB",
                    "BCM",
                    "BID",
                    "BVH",
                    "CTG",
                    "FPT",
                    "GAS",
                    "GVR",
                    "HDB",
                    "HPG",
                    "MBB",
                    "MSN",
                    "MWG",
                    "PLX",
                    "POW",
                    "SAB",
                    "SHB",
                    "SSB",
                    "SSI",
                    "STB",
                    "TCB",
                    "TPB",
                    "VCB",
                    "VHM",
                    "VIB",
                    "VIC",
                    "VJC",
                    "VNM",
                    "VPB",
                    "VRE"
                ]
            },
            "outputs": {
                "output": {
                    "type": "line",
                    "mapping": "MA",
                    "value": "MA",
                    "color": "#788d2a"
                }
            },
            "params": {
                "window": 10,
                "ma_type": "SMA"
            }
        },
        {
            "function": "MA",
            "inputs": {
                "src": "fSellVal",
                "rolling_window": 1,
                "rolling_method": "sum",
                "daily_rolling": true,
                "exclude_atc": false,
                "scale": 1000,
                "timeframe": "30S",
                "stocks": [
                    "ACB",
                    "BCM",
                    "BID",
                    "BVH",
                    "CTG",
                    "FPT",
                    "GAS",
                    "GVR",
                    "HDB",
                    "HPG",
                    "MBB",
                    "MSN",
                    "MWG",
                    "PLX",
                    "POW",
                    "SAB",
                    "SHB",
                    "SSB",
                    "SSI",
                    "STB",
                    "TCB",
                    "TPB",
                    "VCB",
                    "VHM",
                    "VIB",
                    "VIC",
                    "VJC",
                    "VNM",
                    "VPB",
                    "VRE"
                ]
            },
            "outputs": {
                "output": {
                    "type": "line",
                    "mapping": "MA",
                    "value": "MA2",
                    "color": "#788d2a"
                }
            },
            "params": {
                "window": 10,
                "ma_type": "SMA"
            }
        }
    ]


    LOAD_PROCESS_SERIES_DATA = [
        {
            "function": "absolute_change_in_range",
            "inputs": {
                "src": "fF1BuyVol",
                "rolling_window": 1,
                "rolling_method": "sum",
                "daily_rolling": true,
                "timeframe": "30S"
            },
            "params": {
                "n_bars": 1,
                "lower_thres": 1000,
                "upper_thres": 99999,
                "use_as_lookback_cond": false,
                "lookback_cond_nbar": 5
            }
        }
    ]

    LOAD_PROCESS_DAILY_DATA = [
        {
            "function": "percent_change_in_range",
            "inputs": {
                "src": "F1Close",
                "rolling_window": 1,
                "rolling_method": "sum",
                "daily_rolling": true,
                "timeframe": "30S"
            },
            "params": {
                "n_bars": 1,
                "lower_thres": 3,
                "upper_thres": 100,
                "use_as_lookback_cond": false,
                "lookback_cond_nbar": 5
            }
        }
    ]


    LOAD_PROCESS_STOCKS_DATA = [
        {
            "function": "two_line_pos",
            "inputs": {
                "src1": "bu2", 
                "src2": "sd2",
                "timeframe": "15Min", 
                "rolling_window": 3,
                "rolling_method": "sum",
                "daily_rolling": False
            },
            "params": {
                "direction": "crossover"
            }
        }
    ]


    COMBINE_TEST = [
        {
            'function': "is_in_top_bot_percentile",
            'inputs': {
                'src': 'bu2',
                'rolling_window': 20,
                'rolling_method': 'sum',
                'daily_rolling': False
            },
            'params': {
                'lookback_period': 50,
                'direction': 'top',
                'threshold': 90,
                'use_as_lookback_cond': False,
                'lookback_cond_nbar': 5
            }
        },
        {
            'function': 'two_line_pos',
            'inputs': {
                'src1': 'bu2',
                'src2': 'sd2',
                'rolling_window': 20,
                'daily_rolling': False
            },
            'params': {
                'direction': 'crossover'
            }
        },
    ]


    LOAD_DATA = {
        "group_params": [],
        "other_params": [
            {
                "function": "MA",
                "inputs": {
                    "src": "F1Value",
                    "rolling_window": 1,
                    "rolling_method": "sum",
                    "daily_rolling": true,
                    "timeframe": "30S"
                },
                "outputs": {
                    "output": {
                        "type": "line",
                        "mapping": "MA",
                        "value": "MA",
                        "color": "#f5a35e"
                    }
                },
                "params": {
                    "window": 1,
                    "ma_type": "SMA"
                }
            }
        ],
        "dailyindex_params": [],
        "start_day": "2025_03_12",
        "end_day": "2025_03_19"
    }