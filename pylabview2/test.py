from danglib.pylabview2.funcs import *
name = 'new_combination_os'
store_folder = f"/data/Tai/{name}"
maybe_create_dir(store_folder)

result_folder = f"/data/Tai/pickles/{name}"
maybe_create_dir(result_folder)

recompute_signals = True
## CALC  -------------------------------------------------------------------

stocks_map, day_ls = Vectorized.calc_and_push_data_to_plasma(
                                    push_return_numpy=True, 
                                    push_stocks_numpy=True
                                )

n_strats_env = Vectorized.MultiProcess.compute_signals_env(recompute_signals)
n_strats_trigger = Vectorized.MultiProcess.compute_signals_trigger(recompute_signals)

Vectorized.MultiProcess.compute_wr_re_nt_new_combination_os(n_strats_env, folder=store_folder)
Vectorized.JoinResults.join_wr_re_nt_data_cfm(stocks_map, src_folder=store_folder, des_folder=result_folder)