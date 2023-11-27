import pandas as pd
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')

db = client['tpch']

part = db['part'].find()
lineitem = db['lineitem'].find()

df_part = pd.DataFrame(list(part))
df_lineitem = pd.DataFrame(list(lineitem))

merged_df = pd.merge(df_lineitem, df_part, how="inner", left_on="L_PARTKEY", right_on="P_PARTKEY")

merged_df['L_SHIPDATE'] = pd.to_datetime(merged_df['L_SHIPDATE'])

filtered_df = merged_df.loc[(merged_df['L_SHIPDATE'] >= '1995-09-01') & (merged_df['L_SHIPDATE'] < '1995-10-01')]

filtered_df['PROMO_REVENUE'] = filtered_df.apply(lambda row: (row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT'])) 
                                                 if row['P_TYPE'].startswith('PROMO') else 0, axis=1)

filtered_df['NORMAL_REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])

filtered_output = (100.00 * filtered_df['PROMO_REVENUE'].sum()) / filtered_df['NORMAL_REVENUE'].sum()

df_output = pd.DataFrame({"PROMO_REVENUE": [filtered_output]})
df_output.to_csv("query_output.csv", index=False)

print('Query output has been written in the file query_output.csv')
