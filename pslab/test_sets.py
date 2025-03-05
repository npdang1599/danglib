from danglib.pslab.resources import Globs

class TestSets:
    LOAD_PROCESS_GROUP_DATA = [
        {
            'function': "is_in_top_bot_percentile",
            'inputs': {
                'src': 'bu2',
                'stocks': Globs.SECTOR_DIC['VN30'],
                # 'timeframe': '15Min',
                'rolling_window': 20,
                'rolling_method': 'sum'
            },
            'params': {
                'lookback_period': 1000,
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
                'stocks': ['HPG', 'SSI', 'NVL'],
                # 'timeframe': '15Min',
                'rolling_window': 20
            },
            'params': {
                'direction': 'crossover'
            }
        },
    ]


    LOAD_PROCESS_SERIES_DATA = [
        {
            "function": "absolute_change_in_range",
            "inputs": {
            "src": "VnindexClose",
            "daily_rolling": False
            # "timeframe": "15Min",
            },
            "params": {
            "n_bars": 5,
            "lower_thres": 1,
            "upper_thres": 999,
            "use_as_lookback_cond": False,
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
                "direction", "crossover"
            }
        }
    ]


    COMBINE_TEST = [
        {
            'function': "is_in_top_bot_percentile",
            'inputs': {
                'src': 'bu2',
                'stocks': ['VN30', 'Super High Beta'],
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
        # {
        #     'function': 'gap_trend',
        #     'inputs': {
        #         'src1': 'bid',
        #         'src2': 'ask',
        #         'stocks': ['HPG', 'SSI', 'NVL']
        #     },
        #     'params': {
        #         'trend_direction': 'increase',
        #         'trend_bars': 5
        #     }
        # },
        {
            'function': 'two_line_pos',
            'inputs': {
                'src1': 'bu2',
                'src2': 'sd2',
                'stocks': ['VN30', 'Super High Beta'],
                'rolling_window': 20,
                'daily_rolling': False
            },
            'params': {
                'direction': 'crossover'
            }
        },
    ]


    LOAD_DATA = {
        "group_params": [
            {
                "function": "MA",
                "inputs": {
                    "src": "ask",
                    "rolling_window": 5,
                    "rolling_method": "sum",
                    "timeframe": "30Min",
                    "stocks": [
                        "Super High Beta"
                    ]
                },
                "outputs": {
                    "output": {
                        "type": "line",
                        "mapping": "MA",
                        "value": "MA",
                        "color": "#f88962"
                    }
                },
                "params": {
                    "window": 10,
                    "ma_type": "SMA"
                }
            },
            {
                "function": "bbwp",
                "inputs": {
                    "src": "fBuyVal",
                    "rolling_window": 1,
                    "rolling_method": "sum",
                    "timeframe": "30Min",
                    "stocks": [
                        "High Beta"
                    ]
                },
                "outputs": {
                    "output": {
                        "type": "line",
                        "mapping": "bbwp",
                        "value": "bbwp",
                        "color": "#048ae0"
                    }
                },
                "params": {
                    "basic_type": "SMA",
                    "bbwp_len": 13,
                    "bbwp_lkbk": 128
                }
            }
        ],
        "other_params": [
            {
                "function": "ursi",
                "inputs": {
                    "src": "Arbit",
                    "rolling_window": 1,
                    "rolling_method": "sum",
                    "timeframe": "30Min"
                },
                "outputs": {
                    "output1": {
                        "type": "line",
                        "mapping": "arsi",
                        "value": "ursi",
                        "color": "#8c6794"
                    },
                    "output2": {
                        "type": "line",
                        "mapping": "arsi_signal",
                        "value": "ursi_signal",
                        "color": "#5e65a7"
                    }
                },
                "params": {
                    "length": 14,
                    "smo_type1": "RMA",
                    "smooth": 14,
                    "smo_type2": "EMA"
                }
            }
        ],
        "dailyindex_params": [
            {
                "function": "macd",
                "inputs": {
                    "src": "F1Close",
                    "timeframe": "30Min"
                },
                "outputs": {
                    "output1": {
                        "type": "line",
                        "mapping": "macd",
                        "value": "macd",
                        "color": "#34d18b"
                    },
                    "output2": {
                        "type": "line",
                        "mapping": "macd_signal",
                        "value": "macd_signal",
                        "color": "#1746dd"
                    }
                },
                "params": {
                    "r2_period": 20,
                    "fast": 10,
                    "slow": 20,
                    "signal_length": 9
                }
            }
        ],
        "start_day": "2025_01_28",
        "end_day": "2025_02_04"
    }