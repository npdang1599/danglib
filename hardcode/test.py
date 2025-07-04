true = True
false = False
strategy = {
    "name": "1Min-f1Bid giảm 15/15 nến + 1Min-bu2 tổng giá trị trong 5 nến lớn hơn 700",
    "id": 152,
    "conditions": {
      "countline_params": [],
      "group_params": [
        {
          "function": "range_nbars",
          "inputs": {
            "src": "bu2",
            "timeframe": "1Min",
            "rolling_window": 10,
            "rolling_method": "mean",
            "daily_rolling": true,
            "exclude_atc": false,
            "stocks": [
              "All"
            ]
          },
          "params": {
            "lower_thres": 700.0,
            "upper_thres": 1000000000000000000,
            "sum_nbars": 5,
            "use_as_lookback_cond": false,
            "lookback_cond_nbar": 5
          }
        }
      ],
      "other_params": [
        {
          "function": "min_inc_dec_bars",
          "inputs": {
            "src": "f1Bid",
            "timeframe": "1Min",
            "rolling_window": 120,
            "rolling_method": "mean",
            "daily_rolling": true,
            "exclude_atc": false
          },
          "params": {
            "n_bars": 15,
            "n_bars_inc": 0,
            "n_bars_dec": 15,
            "use_as_lookback_cond": false,
            "lookback_cond_nbar": 5
          }
        }
      ],
      "dailyindex_params": [],
      "lookback_periods": 120,
      "index": "F1",
      "close_ATC": false,
      "start_date": "2022_01_01",
      "end_date": "2025_06_26",
      "timeframe": "1Min"
    },
    "holding_periods": "120Min",
    "Number of Trades": 250,
    "Number Entry Days": 40,
    "Win Rate": 76.4,
    "Average Return": 3.741599999999997,
    "group": [
      "BUSD",
      "combine",
      "120Min"
    ],
    "type": "short"
  }