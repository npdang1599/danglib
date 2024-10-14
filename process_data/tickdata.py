# from danglib.Adapters.adapters import MongoAdapters

# df = MongoAdapters.load_hose500_from_db('2024_10_11')
import pandas as pd

def load_ssi_hose_eod_from_db(day):
    from pymongo import MongoClient
    # day = '2022_08_30'
    client = MongoClient('cmc', 27017)
    db = client['dc_data']
    coll_name = f""{day}_SSI_hose""
    data = list(db[coll_name].find({}, {""_id"": 0}))
    list_df = [pd.DataFrame(data[i]) for i in range(len(data))]
    df = pd.concat(list_df,ignore_index=True)
    return df