import pandas as pd, numpy as np
from typing import Dict, Union
PandasObject = Union[pd.Series, pd.DataFrame]

class Ta:
    @staticmethod 
    def sma(src: PandasObject, window: int):
        """Simple moving average
        
        Args:
            src: pd.Series or pd.DataFrame
            window: int, moving average period
            
        Returns:
            Simple moving average values
        """
        return src.rolling(window=window).mean()
    
    @staticmethod
    def stdev(source: PandasObject, length: int):
        """Calculating standard deviation

        Args:
            source (pd.Series): Series of values to process
            length (int): Number of bars (length)

        Returns:
            pd.Series: source.rolling(length).std()
        """
        return source.rolling(length).std(ddof=0)


class Math:
    """mathematical functions for pandas Series"""

    @staticmethod
    def max(df_ls: list[pd.DataFrame]):
        """To calculate the maximum value for each row from multiple Pandas dataframe.

        Args:
            dataframe_ls (list): A sequence of dataframe to use in the calculation.

        Returns:
            pd.dataframe: The dataframe of maximum values for each row of the input dataframe
        """
        max_df = np.maximum.reduce([df.to_numpy() for df in df_ls])
        max_df = pd.DataFrame(max_df, columns=df_ls[1].columns, index=df_ls[1].index)

        return max_df

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

    p_h = df["high"]
    p_l = df["low"]
    p_c = df["close"]
    src = df[src_name]

    basic = Ta.sma(src, bb_length)
    dev = mult_kc * Ta.stdev(src, bb_length)
    upper_bb = basic + dev
    lower_bb = basic - dev

    sqz_ma = Ta.sma(src, length_kc)

    sqz_range = (
        Math.max([p_h - p_l, abs(p_h - p_c.shift(1)), abs(p_l - p_c.shift(1))])
        if use_true_range
        else (p_h - p_l)
    )

    rangema = Ta.sma(sqz_range, length_kc)
    upper_kc = sqz_ma + rangema * mult_kc
    lower_kc = sqz_ma - rangema * mult_kc

    sqz_on :pd.DataFrame = (lower_bb > lower_kc) & (upper_bb < upper_kc)
    sqz_off:pd.DataFrame = (lower_bb < lower_kc) & (upper_bb > upper_kc)
    no_sqz = pd.DataFrame(
        np.where(sqz_on | sqz_off, False, True),
        columns=sqz_on.columns,
        index=sqz_on.index
    )
    def test2():
        dft = sqz_on['ABB'].copy().to_frame('sqz_on')
        dft['sqz_ma'] = sqz_ma['ABB']
        dft['sqz_range'] = sqz_range['ABB']
        dft['rangema'] = rangema['ABB']
        dft['lower_bb'] = lower_bb['ABB']
        dft['lower_kc'] = lower_kc['ABB']
        dft['upper_bb'] = upper_bb['ABB']
        dft['upper_kc'] = upper_kc['ABB']
        dft.loc['2021_01_25']

    return sqz_on, sqz_off, no_sqz


def bbwp(
    df: pd.DataFrame,
    src_name: str = "close",
    basic_type: str = "SMA",
    bbwp_len: int = 13,
    bbwp_lkbk: int = 128,
):
    def test():
        src_name: str = "close"
        basic_type: str = "SMA"
        bbwp_len: int = 13
        bbwp_lkbk: int = 128

    """bbwp"""
    _price = df[src_name]

    _basic = Ta.ma(_price, bbwp_len, basic_type)
    _dev = Ta.stdev(_price, bbwp_len)
    _bbw = (_basic + _dev - (_basic - _dev)) / _basic

    # _bbw_sum = (_bbw.rolling(bbwp_lkbk).rank() - 1) / (bbwp_lkbk - 1)
    # _bbw_sum = (_bbw.rolling(bbwp_lkbk + 1).rank() - 1) / bbwp_lkbk
    index = df.reset_index(drop=True).index
    bbwp_denominator = pd.Series(np.where(index < bbwp_lkbk, index, bbwp_lkbk), index=df.index)
    _bbw_sum = (_bbw.rolling(bbwp_lkbk + 1, min_periods= bbwp_len).rank() - 1).div(bbwp_denominator, axis=0)

    return _bbw_sum * 100
