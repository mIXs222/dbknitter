from pymongo import MongoClient
import pandas as pd
from pandas.io.json import json_normalize

client = MongoClient('mongodb://mongodb:27017/')
db = client['tpch']

col_nation = db['nation']
col_region = db['region']
col_part = db['part']
col_supplier = db['supplier']
col_lineitem = db['lineitem']
col_orders = db['orders']
col_customer = db['customer']

nation_data = list(col_nation.find({}))
region_data = list(col_region.find({}))
part_data = list(col_part.find({}))
supplier_data = list(col_supplier.find({}))
lineitem_data = list(col_lineitem.find({}))
orders_data = list(col_orders.find({}))
customer_data = list(col_customer.find({}))

df_nation = pd.json_normalize(nation_data)
df_region = pd.json_normalize(region_data)
df_part = pd.json_normalize(part_data)
df_supplier = pd.json_normalize(supplier_data)
df_lineitem = pd.json_normalize(lineitem_data)
df_orders = pd.json_normalize(orders_data)
df_customer = pd.json_normalize(customer_data)

# Merge tables together similar to SQL joins
df_merge = df_lineitem.merge(df_part, left_on='L_PARTKEY', right_on='P_PARTKEY')
df_merge = df_merge.merge(df_supplier, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
df_merge = df_merge.merge(df_orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
df_merge = df_merge.merge(df_customer, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
df_merge = df_merge.merge(df_nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY', suffixes=('', '_nation_customer'))
df_merge = df_merge.merge(df_nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY', suffixes=('', '_nation_supplier'))
df_merge = df_merge.merge(df_region, left_on='N_REGIONKEY', right_on='R_REGIONKEY')

# Filter for specific conditions
df_merge = df_merge[
    (df_merge['P_TYPE'] == 'SMALL PLATED COPPER') & 
    (df_merge['R_NAME'] == 'ASIA') & 
    (df_merge['O_ORDERDATE'].between('1995-01-01', '1996-12-31'))]

df_merge['O_YEAR'] = df_merge['O_ORDERDATE'].dt.year
df_merge['VOLUME'] = df_merge['L_EXTENDEDPRICE'] * (1 - df_merge['L_DISCOUNT'])
df_merge['NATION'] = df_merge['N_NAME_nation_supplier']

# Group by and calculate the final result
df_result = df_merge.groupby('O_YEAR').apply(lambda group: group[group['NATION'] == 'INDIA']['VOLUME'].sum() / group['VOLUME'].sum()).reset_index(name='MKT_SHARE')
df_result.sort_values('O_YEAR', inplace=True)
df_result.to_csv('query_output.csv', index=False)
