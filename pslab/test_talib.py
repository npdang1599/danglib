import pandas as pd
import numpy as np
from typing import Dict, Union, List
from danglib.pslab.resources import Adapters




PandasObject = Union[pd.Series, pd.DataFrame]

class Ta:
    @staticmethod 
    def sma(src: PandasObject, window: int):
        return src.rolling(window=window).mean()
    
    @staticmethod
    def stdev(source: PandasObject, length: int):
        return source.rolling(length).std(ddof=0)

class Math:
    @staticmethod
    def max(df_ls: List[PandasObject]) -> PandasObject:
        """Calculate maximum value between multiple pandas objects (Series or DataFrames)
        
        Args:
            df_ls (List[PandasObject]): List of Series or DataFrames to compare
            
        Returns:
            PandasObject: Maximum values in same format as input
        """
        # Check if inputs are Series
        if all(isinstance(x, pd.Series) for x in df_ls):
            return pd.Series(np.maximum.reduce([x.values for x in df_ls]), 
                           index=df_ls[0].index)
        
        # If DataFrames
        return pd.DataFrame(
            np.maximum.reduce([df.to_numpy() for df in df_ls]),
            columns=df_ls[0].columns, 
            index=df_ls[0].index
        )

def squeeze(
    df: pd.DataFrame,
    src_name: str = "close",
    bb_length: int = 20,
    length_kc: int = 20,
    mult_kc: float = 1.5,
    use_true_range: bool = True,
):
    """Calculate squeeze indicator for both regular and multi-index DataFrames
    
    Args:
        df (pd.DataFrame): OHLCV DataFrame (regular or multi-index columns)
        src_name (str): Column name for source values
        bb_length (int): Bollinger Bands length
        length_kc (int): Keltner Channel length
        mult_kc (float): Keltner Channel multiplier
        use_true_range (bool): Use true range for KC calculation
        
    Returns:
        tuple: (sqz_on, sqz_off, no_sqz) Series or DataFrames depending on input
    """
    
    # For regular DataFrame, get Series
    p_h = df['high']
    p_l = df['low']
    p_c = df['close']
    src = df[src_name]

    # Calculate BB
    basic = Ta.sma(src, bb_length)
    dev = mult_kc * Ta.stdev(src, bb_length)
    upper_bb = basic + dev
    lower_bb = basic - dev

    # Calculate KC
    sqz_ma = Ta.sma(src, length_kc)
    
    # Calculate range
    if use_true_range:
        sqz_range = Math.max([
            p_h - p_l,
            (p_h - p_c.shift(1)).abs(),
            (p_l - p_c.shift(1)).abs()
        ])
    else:
        sqz_range = p_h - p_l

    rangema = Ta.sma(sqz_range, length_kc)
    upper_kc = sqz_ma + rangema * mult_kc
    lower_kc = sqz_ma - rangema * mult_kc

    # Calculate squeeze conditions
    sqz_on = (lower_bb > lower_kc) & (upper_bb < upper_kc)
    sqz_off = (lower_bb < lower_kc) & (upper_bb > upper_kc)
    no_sqz = ~(sqz_on | sqz_off)

    return sqz_on, sqz_off, no_sqz


regular_df = Adapters.load_stock_data_from_plasma()

sqz_on, sqz_off, no_sqz = squeeze(regular_df)


