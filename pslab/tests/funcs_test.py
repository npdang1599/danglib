from danglib.pslab.funcs import CombiConds, TestSets



required_data, conditions_params = CombiConds.load_and_process_group_data(TestSets.LOAD_PROCESS_GROUP_DATA)

# Kết hợp điều kiện
new_combined = CombiConds.combine_conditions(required_data=required_data, conditions_params=conditions_params, combine_calculator='and')

for k, v in required_data.items():
    print(k, len(v))