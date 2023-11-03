from pymongo import MongoClient
import pandas as pd
from datetime import datetime

client = MongoClient('mongodb://mongodb:27017/')
db = client['tpch']

lineitems = db.lineitem.find({'L_SHIPDATE': {'$lte': datetime(1998, 9, 2)}}, {'_id': 0})

df_lineitem = pd.DataFrame(list(lineitems))

df_lineitem["L_EXTENDEDPRICE"] = df_lineitem["L_EXTENDEDPRICE"].astype(float)
df_lineitem["L_DISCOUNT"] = df_lineitem["L_DISCOUNT"].astype(float)
df_lineitem["L_TAX"] = df_lineitem["L_TAX"].astype(float)

df_result = df_lineitem.groupby(['L_RETURNFLAG', 'L_LINESTATUS']).agg({'L_QUANTITY': ['sum', 'mean'], 'L_EXTENDEDPRICE': ['sum', 'mean'], 'L_DISCOUNT': 'mean', 'L_LINESTATUS': 'count'})

df_result["SUM_DISC_PRICE"] = df_result[('L_EXTENDEDPRICE', 'sum')] * (1 - df_result[('L_DISCOUNT', 'mean')])
df_result["SUM_CHARGE"] = df_result["SUM_DISC_PRICE"] * (1 + df_result[('L_TAX', 'mean')])

df_result.columns = ['SUM_QTY', 'AVG_QTY', 'SUM_BASE_PRICE', 'AVG_PRICE', 'AVG_DISC', 'COUNT_ORDER', 'SUM_DISC_PRICE', 'SUM_CHARGE']

df_result = df_result.sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'])

df_result.to_csv('query_output.csv', index=True, header=True)
