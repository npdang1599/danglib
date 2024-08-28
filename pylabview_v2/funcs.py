from danglib.pylabview.funcs import Conds as SConds, glob_obj
from danglib.pylabview_v2.core_lib import pd, np, Ta, Utils, Adapters, Math, Fns

class Conds:
    @staticmethod
    def price_change(
        df: pd.DataFrame,
        src_name: str = "close",
        periods: int = 1,
        direction: str = "increase",
        lower_thres: float = 0,
        upper_thres: float = 10000,
        use_flag: bool = True,
    ):
        """Check if the percentage change in price over a period of time
        falls within the specified range.

        Args:
            df (pd.DataFrame): ohlcv dataframe
            periods (int): period
            direction (str): "increase" or "decrease"
            lower_thres (float): lower range threshold
            upper_thres (float): upper range threshold

        Returns:
            pd.Series[bool]: True or False if use_flag is True else None
        """
        periods = int(periods)
        src = df[src_name]
        if use_flag:
            pct_change = src.pct_change(periods=periods).round(6) * 100
            if direction == "decrease":
                pct_change = pct_change * -1

            return Utils.in_range(pct_change, lower_thres, upper_thres, equal=False)

        return None
    

def pivot_df_stocks():
    df_stocks = glob_obj.df_stocks
    df_stocks = df_stocks.pivot(index='day', columns='stock')
    df_stocks['day'] = df_stocks.index
    return df_stocks


def get_functions(func_name):
    funcs_dic = {
        'price_change': (SConds.price_change, Conds.price_change),
    }
    return funcs_dic[func_name]

stock = 'ABB'



df_stocks_pv = pivot_df_stocks()
df_stocks = glob_obj.df_stocks

func_name = 'price_change'
params = {
    "periods":  10,
    "direction":  "increase",
    "lower_thres":  5,
    "upper_thres":  10000,
    "use_flag":  True,
}

f1, f2 = get_functions(func_name)

print("Error on stocks: ")
count = 0
for stock in df_stocks['stock'].unique():

    df_one_stock = glob_obj.get_one_stock_data(stock).set_index('day')
        
    r1: pd.Series = f1(df_one_stock, **params)
    r2_all = f2(df_stocks_pv, **params)
    r2: pd.Series = r2_all[stock]


    df1 = r1.to_frame('c1')
    df2 = r2.to_frame('c2')

    dfc = df1.merge(df2, how='outer', on='day')
    
    miss = dfc[(dfc['c1'] != dfc['c2']) & (dfc['c1'] | dfc['c2'])]

    
    if len(miss) != 0:
        print(f"{stock}: {len(miss)}", end='\n' if count % 10 == 0 else ', ')
        count += 1
