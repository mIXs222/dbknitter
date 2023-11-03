from pymongo import MongoClient
import pandas as pd
from pandas.io.json import json_normalize

client = MongoClient("mongodb://mongodb:27017/")
db = client['tpch']

partsupp = list(db.partsupp.find())
part = list(db.part.find())
supplier = list(db.supplier.find())
filtered_supplier_keys = [s['S_SUPPKEY'] for s in supplier if 'Customer Complaints' not in s['S_COMMENT']]

incl_sizes = {49, 14, 23, 45, 19, 3, 36, 9}

result_list = []
for p in part:
    if p['P_BRAND'] != 'Brand#45' and not p['P_TYPE'].startswith('MEDIUM POLISHED') and p['P_SIZE'] in incl_sizes:
        for ps in partsupp:
            if ps['PS_PARTKEY'] == p['P_PARTKEY'] and ps['PS_SUPPKEY'] not in filtered_supplier_keys:
                result_list.append({
                    "P_BRAND": p['P_BRAND'],
                    "P_TYPE": p['P_TYPE'],
                    "P_SIZE": p['P_SIZE'],
                    "SUPPLIER_CNT": len(set(ps['PS_SUPPKEY'])),
                })

df = pd.DataFrame(result_list)
aggregated = df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])['SUPPLIER_CNT'].nunique()
aggregated = aggregated.reset_index().sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])
aggregated.to_csv('query_output.csv', index=False)
