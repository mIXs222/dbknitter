import pymongo
from pymongo import MongoClient
import pandas as pd
from pandas.io.json import json_normalize
from datetime import datetime

# Connecting to pymongo
client = MongoClient(host='mongodb', port=27017, username='user', password='password')

# Accessing the tpch database
db = client.tpch

# Loading collections
nation = db.nation
region = db.region
part = db.part
supplier = db.supplier
customer = db.customer
orders = db.orders
lineitem = db.lineitem

# Converting collections from bson to pandas DataFrames
nation_df = json_normalize(list(nation.find()))
region_df = json_normalize(list(region.find()))
part_df = json_normalize(list(part.find()))
supplier_df = json_normalize(list(supplier.find()))
customer_df = json_normalize(list(customer.find()))
orders_df = json_normalize(list(orders.find()))
lineitem_df = json_normalize(list(lineitem.find()))

# Converting date from iso to datetime
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE']).dt.strftime('%Y-%m-%d')

# Merging all tables
df = pd.merge(lineitem_df, part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')
df = pd.merge(df, supplier_df, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')
df = pd.merge(df, orders_df, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
df = pd.merge(df, customer_df, how='inner', left_on='O_CUSTKEY', right_on='C_CUSTKEY')
df = pd.merge(df, nation_df, how='inner', left_on='C_NATIONKEY', right_on='N_NATIONKEY', suffixes=('', '_nation'))
df = pd.merge(df, region_df, how='inner', left_on='N_REGIONKEY', right_on='R_REGIONKEY')
df = df.rename(columns={'N_NAME': 'NATION'})

# Applying the query
df_result = df[
    (df['R_NAME'] == 'ASIA') &
    (df['S_NATIONKEY'] == df['N_NATIONKEY']) &
    (df['O_ORDERDATE'].between('1995-01-01', '1996-12-31')) &
    (df['P_TYPE'] == 'SMALL PLATED COPPER')
].copy()

df_result['VOLUME'] = df_result['L_EXTENDEDPRICE'] * (1 - df_result['L_DISCOUNT'])
df_result['O_YEAR'] = pd.DatetimeIndex(df_result['O_ORDERDATE']).year

df_grouped = df_result.groupby('O_YEAR').apply(lambda grp: grp[grp['NATION'] == 'INDIA']['VOLUME'].sum() /  grp['VOLUME'].sum()).reset_index()
df_grouped.columns = ['O_YEAR', 'MKT_SHARE']

df_grouped.to_csv('query_output.csv', index=False)
