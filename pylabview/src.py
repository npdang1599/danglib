class Fns:
    # pickle_stocks_data = "/home/ubuntu/Dang/pylab_tview/map_income_df2.pickle"
    # stock_beta_group = "/home/ubuntu/Dang/pylab_tview/All Ticker_Beta_May2024.xlsx"

    pickle_stocks_data = "/home/ubuntu/Dang/data/pylabview_stocks_data.pickle"
    stock_beta_group = "/home/ubuntu/Dang/data/All Ticker_Beta_May2024.xlsx"
    
    sectors_rawdata = {
        'brokerage': {
            'margin_lending': {
                'io':'/home/ubuntu/Dang/data/Margin lending.xlsx',
                'sheet_name':'Sheet1',
                'skiprows':range(1,2)
            },
            'bank_rates':{
                'io':'/home/ubuntu/Dang/data/bank_rates.xlsx',
                'sheet_name': 'Sheet2',
                'skiprows': range(1,7)
            }
        },
        'fish':{
            'fish_data': {
                'io': '/home/ubuntu/Dang/data/Fish data for trading view 2024_1.xlsx',
                'sheet_name': 'Consolidated data'
            }
        },
        'hog':{
            'hog_data':{
                'io': '/home/ubuntu/Dang/data/Hog data sets_5 Jul 2024.xlsx',
                'sheet_name': 'Sheet1'
            }
        },
        'fertilizer':{
            'p4_price': {
                'io': '/home/ubuntu/Dang/data/P4 Price (Fertilizer).xlsx'
            },
            'DCM_urea': {
                'io': '/home/ubuntu/Dang/data/Urea Data.xlsx',
                'sheet_name': 'DCM',
                'usecols': [0,2]
            },
            'urea_future': {
                'io': '/home/ubuntu/Dang/data/Urea Data.xlsx',
                'sheet_name': 'Urea Future',
                'usecols': [0,1]
            },
            'DPM_urea': {
                'io': '/home/ubuntu/Dang/data/Urea Data.xlsx',
                'sheet_name': 'DPM',
                'usecols': [0,2]
            }
        }
    }

    
    

sectors_paths = {
    'steel': {
        'categories': {
            'path':'/home/ubuntu/Dang/data/FA Data_HPG.xlsx',
            'sheet_name':'Categories'
        },
        'io_data': {
            'path':'/home/ubuntu/Dang/data/FA Data_HPG.xlsx',
            'sheet_name':'Sheet2',
            'header':2
        },
    }
}



