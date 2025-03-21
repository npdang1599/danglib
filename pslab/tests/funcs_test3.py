import pandas as pd
import numpy as np
from danglib.pslab.test_sets import TestSets
from danglib.pslab.tests.funcs_test2 import CombiConds2, CombiConds, new_version, old_version

def are_equal(obj1, obj2, rtol=1e-5, atol=1e-8):
    """
    Kiểm tra xem hai đối tượng pandas (DataFrame hoặc Series) có giống nhau không.
    
    Parameters:
    -----------
    obj1, obj2 : pd.DataFrame hoặc pd.Series
        Hai đối tượng cần so sánh
    rtol : float, mặc định=1e-5
        Dung sai tương đối cho so sánh số thực
    atol : float, mặc định=1e-8
        Dung sai tuyệt đối cho so sánh số thực
    
    Returns:
    --------
    bool, str
        True/False và thông báo chi tiết
    """
    # Kiểm tra kiểu dữ liệu
    if type(obj1) != type(obj2):
        return False, f"Kiểu dữ liệu khác nhau: {type(obj1)} vs {type(obj2)}"
    
    # Xử lý DataFrame
    if isinstance(obj1, pd.DataFrame):
        obj1 = obj1.copy()
        obj2 = obj2.copy()
        obj1 = obj1.sort_index(axis=0).sort_index(axis=1)
        obj2 = obj2.sort_index(axis=0).sort_index(axis=1)

        # Kiểm tra kích thước
        if obj1.shape != obj2.shape:
            return False, f"Kích thước khác nhau: {obj1.shape} vs {obj2.shape}"
        
        # Kiểm tra tên cột
        if not obj1.columns.equals(obj2.columns):
            return False, "Tên cột khác nhau"
        
        # Kiểm tra index
        if not obj1.index.equals(obj2.index):
            return False, "Index khác nhau"
        
        # Kiểm tra giá trị NaN (vị trí trùng khớp)
        nan_mask1 = obj1.isna()
        nan_mask2 = obj2.isna()
        if not (nan_mask1 == nan_mask2).all().all():
            return False, "Vị trí giá trị NaN khác nhau"
        
        # So sánh giá trị non-NaN 
        for col in obj1.columns:
            non_na_mask = ~nan_mask1[col]
            values1 = obj1.loc[non_na_mask, col].values
            values2 = obj2.loc[non_na_mask, col].values
            
            if not np.allclose(values1, values2, rtol=rtol, atol=atol, equal_nan=True):
                # Tìm vị trí khác nhau
                diff_mask = ~np.isclose(values1, values2, rtol=rtol, atol=atol, equal_nan=True)
                if np.any(diff_mask):
                    diff_indices = np.where(diff_mask)[0]
                    diff_values1 = values1[diff_mask]
                    diff_values2 = values2[diff_mask]
                    sample_diff = f"Ví dụ: {diff_values1[0]} vs {diff_values2[0]}"
                    return False, f"Giá trị khác nhau trong cột {col} tại {len(diff_indices)} vị trí. {sample_diff}"
        
        return True, "Hai DataFrame giống nhau"
    
    # Xử lý Series
    elif isinstance(obj1, pd.Series):
        # Kiểm tra kích thước
        if len(obj1) != len(obj2):
            return False, f"Kích thước khác nhau: {len(obj1)} vs {len(obj2)}"
        
        # Kiểm tra tên 
        if obj1.name != obj2.name:
            return False, f"Tên Series khác nhau: {obj1.name} vs {obj2.name}"
        
        # Kiểm tra index
        if not obj1.index.equals(obj2.index):
            return False, "Index khác nhau"
        
        # Kiểm tra giá trị NaN
        nan_mask1 = obj1.isna()
        nan_mask2 = obj2.isna()
        if not (nan_mask1 == nan_mask2).all():
            return False, "Vị trí giá trị NaN khác nhau"
        
        # So sánh giá trị non-NaN
        non_na_mask = ~nan_mask1
        values1 = obj1[non_na_mask].values
        values2 = obj2[non_na_mask].values
        
        if not np.allclose(values1, values2, rtol=rtol, atol=atol, equal_nan=True):
            # Tìm vị trí khác nhau
            diff_mask = ~np.isclose(values1, values2, rtol=rtol, atol=atol, equal_nan=True)
            if np.any(diff_mask):
                diff_indices = np.where(diff_mask)[0]
                diff_values1 = values1[diff_mask]
                diff_values2 = values2[diff_mask]
                sample_diff = f"Ví dụ: {diff_values1[0]} vs {diff_values2[0]}"
                return False, f"Giá trị khác nhau tại {len(diff_indices)} vị trí. {sample_diff}"
        
        return True, "Hai Series giống nhau"
    
    else:
        return False, f"Kiểu dữ liệu không được hỗ trợ: {type(obj1)}"

def test_new_vs_old_version():
    """So sánh kết quả của new_version và old_version."""
    try:
        new_result = new_version()
        old_result = old_version()
        
        # Kiểm tra kiểu dữ liệu
        if type(new_result) != type(old_result):
            return False, "Kiểu dữ liệu khác nhau"
        
        # So sánh kết quả DataFrame
        if isinstance(new_result, pd.DataFrame) and isinstance(old_result, pd.DataFrame):
            if new_result.shape != old_result.shape:
                return False, "Kích thước DataFrame khác nhau"
            
            # So sánh dữ liệu non-NaN
            mask = ~new_result.isna() & ~old_result.isna()
            if not (new_result[mask] == old_result[mask]).all().all():
                return False, "Giá trị khác nhau trong DataFrame"
        
        return True, "Kết quả new_version và old_version khớp nhau"
    except Exception as e:
        return False, f"Lỗi: {str(e)}"

def test_process_stock_data():
    """So sánh kết quả của load_and_process_stock_data."""
    try:
        # Sửa lỗi cú pháp trong params nếu cần
        stock_params = TestSets.LOAD_PROCESS_STOCKS_DATA
        
        # Chạy phiên bản mới và cũ
        new_data, new_conditions = CombiConds2.load_and_process_stock_data(stock_params)
        old_data, old_conditions = CombiConds.load_and_process_stock_data(stock_params)
        
        # So sánh số lượng keys
        if len(new_data) != len(old_data):
            return False, "Số lượng khóa khác nhau"
        
        # # So sánh dữ liệu
        # for key in new_data:
        #     if key not in old_data:
        #         return False, f"Khóa {key} không có trong phiên bản cũ"
        
        # Kết hợp điều kiện
        new_combined = CombiConds.combine_conditions(new_data, new_conditions, 'and')
        old_combined = CombiConds.combine_conditions(old_data, old_conditions, 'and')


        are_same_df, message_df = are_equal(new_combined, old_combined)

        if not are_same_df:
            return False, message_df
        
        # Kiểm tra kiểu kết quả là DataFrame
        if not isinstance(new_combined, pd.DataFrame) or not isinstance(old_combined, pd.DataFrame):
            return False, "Kết quả kết hợp không phải DataFrame"
        
        return True, "Kết quả process_stock_data khớp nhau"
    except Exception as e:
        return False, f"Lỗi: {str(e)}"

def test_process_group_data():
    """So sánh kết quả của load_and_process_group_data."""
    try:
        # Chạy phiên bản mới và cũ
        new_data, new_conditions = CombiConds2.load_and_process_group_data(TestSets.LOAD_PROCESS_GROUP_DATA)
        old_data, old_conditions = CombiConds.load_and_process_group_data(TestSets.LOAD_PROCESS_GROUP_DATA)
        
        # So sánh số lượng keys
        if len(new_data) != len(old_data):
            return False, "Số lượng khóa khác nhau"
        
        # Kết hợp điều kiện
        new_combined = CombiConds.combine_conditions(new_data, new_conditions, 'and')
        old_combined = CombiConds.combine_conditions(old_data, old_conditions, 'and')

        are_equal(new_combined, old_combined)
        
        # Kiểm tra kiểu kết quả là Series
        if not isinstance(new_combined, pd.Series) or not isinstance(old_combined, pd.Series):
            return False, "Kết quả kết hợp không phải Series"
        
        return True, "Kết quả process_group_data khớp nhau"
    except Exception as e:
        return False, f"Lỗi: {str(e)}"

def test_process_series_data():
    """So sánh kết quả của load_and_process_one_series_data."""
    try:
        # Chạy phiên bản mới và cũ với dữ liệu được cung cấp từ ngoài
        # Lưu ý: Test này giả định rằng dữ liệu có sẵn trong plasma hoặc sẽ được cung cấp khi chạy
        new_data, new_conditions = CombiConds2.load_and_process_one_series_data(TestSets.LOAD_PROCESS_SERIES_DATA)
        old_data, old_conditions = CombiConds.load_and_process_one_series_data(TestSets.LOAD_PROCESS_SERIES_DATA)
        
        # So sánh số lượng keys
        if len(new_data) != len(old_data):
            return False, "Số lượng khóa khác nhau"
        
        # Kết hợp điều kiện 
        new_combined = CombiConds.combine_conditions(new_data, new_conditions, 'and')
        old_combined = CombiConds.combine_conditions(old_data, old_conditions, 'and')

        are_same_df, message_df = are_equal(new_combined, old_combined)

        if not are_same_df:
            return False, message_df
        
        # Kiểm tra kiểu kết quả là Series
        if not isinstance(new_combined, pd.Series) or not isinstance(old_combined, pd.Series):
            return False, "Kết quả kết hợp không phải Series"
        
        return True, "Kết quả process_series_data khớp nhau"
    except Exception as e:
        return False, f"Lỗi: {str(e)}"
    
def test_process_daily_data():
    """So sánh kết quả của load_and_process_one_series_data."""
    try:
        # Chạy phiên bản mới và cũ với dữ liệu được cung cấp từ ngoài
        # Lưu ý: Test này giả định rằng dữ liệu có sẵn trong plasma hoặc sẽ được cung cấp khi chạy
        new_data, new_conditions = CombiConds2.load_and_process_one_series_data(TestSets.LOAD_PROCESS_DAILY_DATA, data_src='daily_index')
        old_data, old_conditions = CombiConds.load_and_process_one_series_data(TestSets.LOAD_PROCESS_DAILY_DATA, data_src='daily_index')
        
        # So sánh số lượng keys
        if len(new_data) != len(old_data):
            return False, "Số lượng khóa khác nhau"
        
        # Kết hợp điều kiện 
        new_combined = CombiConds.combine_conditions(new_data, new_conditions, 'and')
        old_combined = CombiConds.combine_conditions(old_data, old_conditions, 'and')

        are_same_df, message_df = are_equal(new_combined, old_combined)

        if not are_same_df:
            return False, message_df
        
        # Kiểm tra kiểu kết quả là Series
        if not isinstance(new_combined, pd.Series) or not isinstance(old_combined, pd.Series):
            return False, "Kết quả kết hợp không phải Series"
        
        return True, "Kết quả process_series_data khớp nhau"
    except Exception as e:
        return False, f"Lỗi: {str(e)}"

def test_combine_test():
    """So sánh kết quả với COMBINE_TEST."""
    try:
        # Chạy phiên bản mới và cũ với COMBINE_TEST
        new_data, new_conditions = CombiConds2.load_and_process_stock_data(TestSets.COMBINE_TEST)
        old_data, old_conditions = CombiConds.load_and_process_stock_data(TestSets.COMBINE_TEST)
        
        # Kết hợp điều kiện - AND
        new_combined_and = CombiConds.combine_conditions(new_data, new_conditions, 'and')
        old_combined_and = CombiConds.combine_conditions(old_data, old_conditions, 'and')
        
        # Kết hợp điều kiện - OR
        new_combined_or = CombiConds.combine_conditions(new_data, new_conditions, 'or')
        old_combined_or = CombiConds.combine_conditions(old_data, old_conditions, 'or')

        are_same_df_and, message_df_and = are_equal(new_combined_and, old_combined_and)
        if not are_same_df_and:
            return False, message_df_and
        
        are_same_df_or, message_df_or = are_equal(new_combined_or, old_combined_or)
        if not are_same_df_or:
            return False, message_df_or
        
        # Kiểm tra kết quả
        if isinstance(new_combined_and, pd.DataFrame) and isinstance(old_combined_and, pd.DataFrame):
            if new_combined_and.shape != old_combined_and.shape:
                return False, "Kết quả AND có kích thước khác nhau"
        
        if isinstance(new_combined_or, pd.DataFrame) and isinstance(old_combined_or, pd.DataFrame):
            if new_combined_or.shape != old_combined_or.shape:
                return False, "Kết quả OR có kích thước khác nhau"
        
        return True, "Kết quả COMBINE_TEST khớp nhau"
    except Exception as e:
        return False, f"Lỗi: {str(e)}"

def run_all_tests():
    """Chạy tất cả các bài kiểm tra và báo cáo kết quả."""
    tests = [
        # ("So sánh new_version và old_version", test_new_vs_old_version),
        ("Kiểm tra process_stock_data", test_process_stock_data),
        ("Kiểm tra process_group_data", test_process_group_data),
        ("Kiểm tra process_series_data", test_process_series_data),
        ("Kiểm tra process_daily_data", test_process_daily_data),
        # ("Kiểm tra COMBINE_TEST", test_combine_test)
    ]
    
    all_passed = True
    
    for test_name, test_func in tests:
        print(f"\n{'-' * 40}")
        print(f"Thực hiện kiểm tra: {test_name}")
        print(f"{'-' * 40}")
        
        success, message = test_func()
        
        if success:
            print(f"✅ PASS: {message}")
        else:
            print(f"❌ FAIL: {message}")
            all_passed = False
    
    print(f"\n{'-' * 40}")
    if all_passed:
        print("✅ Tất cả các kiểm tra đều thành công!")
    else:
        print("❌ Một số kiểm tra thất bại, xem chi tiết phía trên.")

from danglib.utils import check_run_with_interactive
if __name__ == "__main__" and not check_run_with_interactive():
    run_all_tests()