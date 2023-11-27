# query_script.py

import pymongo
from bson.objectid import ObjectId
import pandas as pd
import direct_redis

# MongoDB connection and data retrieval
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
tpch_db = mongo_client["tpch"]

# Retrieve data from mongodb
nation_df = pd.DataFrame(list(tpch_db["nation"].find()))
region_df = pd.DataFrame(list(tpch_db["region"].find()))
part_df = pd.DataFrame(list(tpch_db["part"].find()))
supplier_df = pd.DataFrame(list(tpch_db["supplier"].find()))

# Redis connection and data retrieval
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
customer_df = pd.read_json(r.get('customer'))
orders_df = pd.read_json(r.get('orders'))
lineitem_df = pd.read_json(r.get('lineitem'))

# Select only interested countries and regions
asia_region = region_df[region_df['R_NAME'] == 'ASIA']['R_REGIONKEY'].iloc[0]
india_nation = nation_df[(nation_df['N_NAME'] == 'INDIA') & (nation_df['N_REGIONKEY'] == ObjectId(str(asia_region)))]['N_NATIONKEY'].iloc[0]

# Merge and query the datasets
merged_supplier = supplier_df[supplier_df['S_NATIONKEY'] == ObjectId(str(india_nation))]
merged_parts = part_df[part_df['P_TYPE'] == 'SMALL PLATED COPPER']
merged_lineitem = lineitem_df.merge(merged_parts, left_on='L_PARTKEY', right_on='_id')
merged_orders = orders_df[(orders_df['O_ORDERDATE'].str.contains('1995')) | (orders_df['O_ORDERDATE'].str.contains('1996'))]

final_merged = merged_lineitem.merge(merged_supplier, left_on='L_SUPPKEY', right_on='_id')
final_merged = final_merged.merge(merged_orders, left_on='L_ORDERKEY', right_on='_id')

final_merged['YEAR'] = pd.to_datetime(final_merged['O_ORDERDATE']).dt.year
final_merged['REVENUE'] = final_merged['L_EXTENDEDPRICE'] * (1 - final_merged['L_DISCOUNT'])

grouped = final_merged.groupby('YEAR')['REVENUE'].sum()
grouped = grouped.reindex([1995, 1996], fill_value=0)
grouped.to_csv('query_output.csv', header=False)
