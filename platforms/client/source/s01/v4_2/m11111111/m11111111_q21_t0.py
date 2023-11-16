from pymongo import MongoClient
import pandas as pd

client = MongoClient('mongodb', 27017)
db = client['tpch']

table1 = pd.DataFrame(list(db['nation'].find()))
table2 = pd.DataFrame(list(db['supplier'].find()))
table3 = pd.DataFrame(list(db['orders'].find()))
table4 = pd.DataFrame(list(db['lineitem'].find()))

merged1 = pd.merge(table2, table4, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
merged2 = pd.merge(merged1, table3, left_on='L_ORDERKEY', right_on='O_ORDERKEY')

df = pd.merge(merged2, table1, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

df_sub = df[df['O_ORDERSTATUS'] == 'F']
df_sub = df_sub[df_sub['L_RECEIPTDATE'] > df_sub['L_COMMITDATE']]

exists = df.loc[df.duplicated(['L_ORDERKEY'], keep=False)]
not_exists = df.loc[~df.duplicated(['L_ORDERKEY', 'L_SUPPKEY', 'L_RECEIPTDATE', 'L_COMMITDATE'], keep=False)]

df_sub = df_sub[df_sub['L_ORDERKEY'].isin(exists['L_ORDERKEY'])]
df_sub = df_sub[~df_sub['L_ORDERKEY'].isin(not_exists['L_ORDERKEY'])]

df_sub = df_sub[df_sub['N_NAME'] == 'SAUDI ARABIA']

df_sub = df_sub.groupby('S_NAME').size().reset_index(name='NUMWAIT')
df_sub = df_sub.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

df_sub.to_csv('query_output.csv', index=False)
