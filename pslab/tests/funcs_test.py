import pandas as pd
import numpy as np
import time
from typing import Tuple, Dict
import logging
from danglib.pslab.resources import Globs, Adapters,

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def calculate_foreign_fubon_old(day: str, df: pd.DataFrame = None) -> pd.DataFrame:
        """
        Calculate foreign transactions for Fubon stocks with high performance optimization.
        
        Args:
            day (str): Date in format 'YYYY_MM_DD'
        
        Returns:
            pd.DataFrame: Processed data excluding put-through transactions
        """
        
        # Load and filter initial data - use vectorized operations
        if df is None:
            df = Adapters.load_data_only(day)

        mask = (df['stock'].isin(Globs.STOCKS)) & (df['time'] <= 10144559) & \
            ~((df['time'] >= 10113059) & (df['time'] <= 10125959))
        cols = ['session', 'time', 'datetime', 'matchingVolume', 'totalMatchVolume',
                    'foreignerBuyVolume', 'foreignerSellVolume', 'stock', 'last']
        df1: pd.DataFrame = df.loc[mask][cols].copy()
        
        # Sort data efficiently
        df1.sort_values(['stock', 'totalMatchVolume', 'foreignerBuyVolume', 'foreignerSellVolume', 'datetime'], inplace=True)
        
        # Vectorized calculation of foreign volume differences
        df1['fBuyVol'] = df1.groupby('stock')['foreignerBuyVolume'].diff().fillna(df1['foreignerBuyVolume'])
        df1['fSellVol'] = df1.groupby('stock')['foreignerSellVolume'].diff().fillna(df1['foreignerSellVolume'])
        
        # Zero out ATO session values vectorized
        df1.loc[df1['session'] == 2, ['fBuyVol', 'fSellVol']] = 0

        
        # Efficient groupby operation
        agg_dict = {
            'session': 'last',
            'datetime': 'last',
            'matchingVolume': 'sum',
            'totalMatchVolume': 'last',
            'foreignerBuyVolume': 'last',
            'foreignerSellVolume': 'last',
            'fBuyVol': 'sum',
            'fSellVol': 'sum',
            'last': 'last'
        }
        df1 = df1.groupby(['stock', 'time'], as_index=False).agg(agg_dict)

        df1['time_str'] = df1['time'].astype(str).map(lambda t: f"{t[2:4]}:{t[4:6]}:{t[6:8]}")
        
        # Process put-through data efficiently
        df_tt = Adapters.load_thoathuan_dc_data_from_db(day)
        if len(df_tt) > 0:
            df_tt_fubon = (df_tt[df_tt['stockSymbol'].isin(Globs.STOCKS)]
                        [['stockSymbol', 'vol', 'createdAt']]
                        .rename(columns={'stockSymbol': 'stock', 'createdAt': 'time_str'}))
            
            df_tt_fubon = (df_tt_fubon.groupby(['stock', 'time_str'])
                        .agg({'vol': 'sum'})
                        .reset_index()
                        .query('time_str <= "14:45:59"'))
            
            # Convert time indices for both dataframes
            df1['x'] = df1['time_str'].map(FRAME.timeToIndex)
            df_tt_fubon['x'] = pd.to_numeric(df_tt_fubon['time_str'].map(FRAME.timeToIndex), errors='coerce')
            df_tt_fubon = df_tt_fubon.dropna()
            df_tt_fubon['x'] = df_tt_fubon['x'].astype(int)
            
            # Use merge_asof on entire dataset instead of loop
            df1 = pd.merge_asof(
                df1.sort_values('x'),
                df_tt_fubon[['stock', 'x', 'vol']].sort_values('x'),
                by='stock',
                on='x',
                tolerance=15,
                direction='nearest'
            )
        else:
            df1['vol'] = np.nan
        
        # Vectorized put-through transaction filtering
        tolerance = 0.002
        pt_mask = ((df1['fBuyVol'].between(df1['vol'] * (1 - tolerance), df1['vol'] * (1 + tolerance))) |
                (df1['fSellVol'].between(df1['vol'] * (1 - tolerance), df1['vol'] * (1 + tolerance))))
        df1['is_pt'] = pt_mask

        return df1[~pt_mask]



class ForeignCalculationBenchmark:
    def __init__(self, day: str):
        self.day = day
        self.original_result = None
        self.new_result = None
        self.performance_metrics = {}

    def load_test_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Load test data for both functions"""
        try:
            # Load raw data for original function
            raw_df = Adapters.load_data_only(self.day)
            
            # Load preprocessed data
            preprocessed_df = ProcessData.preprocess_realtime_stock_data(raw_df)
            
            # Load put-through data
            put_through_df = Adapters.load_thoathuan_dc_data_from_db(self.day)
            
            return raw_df, preprocessed_df, put_through_df
        except Exception as e:
            logger.error(f"Error loading test data: {str(e)}")
            raise

    def run_original_function(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, float]:
        """Run original implementation and measure time"""
        start_time = time.time()
        try:
            result = calculate_foreign_fubon(self.day, df)
            execution_time = time.time() - start_time
            return result, execution_time
        except Exception as e:
            logger.error(f"Error in original function: {str(e)}")
            raise

    def run_new_function(self, df: pd.DataFrame, put_through_df: pd.DataFrame) -> Tuple[pd.DataFrame, float]:
        """Run new implementation and measure time"""
        start_time = time.time()
        try:
            result = Libs.calculate_foreign_fubon(df, put_through_df)
            execution_time = time.time() - start_time
            return result, execution_time
        except Exception as e:
            logger.error(f"Error in new function: {str(e)}")
            raise

    def compare_results(self) -> Dict:
        """Compare results between two implementations"""
        try:
            raw_df, preprocessed_df, put_through_df = self.load_test_data()

            # Run both implementations
            self.original_result, original_time = self.run_original_function(raw_df)
            self.new_result, new_time = self.run_new_function(preprocessed_df, put_through_df)

            # Store performance metrics
            self.performance_metrics = {
                'original_time': original_time,
                'new_time': new_time,
                'time_improvement': f"{((original_time - new_time) / original_time) * 100:.2f}%"
            }

            # Compare results
            comparison = {
                'row_count_match': len(self.original_result) == len(self.new_result),
                'column_differences': self._compare_columns(),
                'value_differences': self._compare_values(),
                'performance': self.performance_metrics
            }

            return comparison

        except Exception as e:
            logger.error(f"Error comparing results: {str(e)}")
            raise

    def _compare_columns(self) -> Dict:
        """Compare columns between results"""
        orig_cols = set(self.original_result.columns)
        new_cols = set(self.new_result.columns)
        
        return {
            'only_in_original': list(orig_cols - new_cols),
            'only_in_new': list(new_cols - orig_cols),
            'common': list(orig_cols & new_cols)
        }

    def _compare_values(self) -> Dict:
        """Compare actual values between results"""
        common_cols = list(set(self.original_result.columns) & set(self.new_result.columns))
        
        # Ensure both DataFrames are sorted the same way
        orig_df = self.original_result.sort_values(['stock', 'timestamp']).reset_index(drop=True)
        new_df = self.new_result.sort_values(['stock', 'timestamp']).reset_index(drop=True)
        
        differences = {}
        for col in common_cols:
            if orig_df[col].dtype in [np.float64, np.float32, np.int64, np.int32]:
                diff = np.abs(orig_df[col] - new_df[col])
                max_diff = diff.max()
                if max_diff > 0:
                    differences[col] = {
                        'max_difference': max_diff,
                        'mean_difference': diff.mean(),
                        'num_differences': (diff > 0).sum()
                    }
        
        return differences

    def print_comparison_report(self):
        """Print a detailed comparison report"""
        comparison = self.compare_results()
        
        print("\n=== Foreign Calculation Comparison Report ===")
        print("\nPerformance Metrics:")
        print(f"Original function time: {comparison['performance']['original_time']:.4f} seconds")
        print(f"New function time: {comparison['performance']['new_time']:.4f} seconds")
        print(f"Performance improvement: {comparison['performance']['time_improvement']}")
        
        print("\nData Structure:")
        print(f"Row count match: {comparison['row_count_match']}")
        
        print("\nColumn Comparison:")
        col_diff = comparison['column_differences']
        print(f"Columns only in original: {col_diff['only_in_original']}")
        print(f"Columns only in new: {col_diff['only_in_new']}")
        print(f"Common columns: {col_diff['common']}")
        
        print("\nValue Differences:")
        for col, diff in comparison['value_differences'].items():
            print(f"\n{col}:")
            print(f"  Max difference: {diff['max_difference']}")
            print(f"  Mean difference: {diff['mean_difference']}")
            print(f"  Number of different values: {diff['num_differences']}")

# Usage example
def run_benchmark(day: str):
    benchmark = ForeignCalculationBenchmark(day)
    benchmark.print_comparison_report()

if __name__ == "__main__":
    run_benchmark("2025_02_12")