# query.py
import pymongo
from bson import json_util
import json
import redis
import pandas as pd
from direct_redis import DirectRedis
import datetime

# MongoDB connection and querying
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongodb = mongo_client["tpch"]

# Query mongodb for relevant tables
customers_df = pd.DataFrame(mongodb["customer"].find())
orders_df = pd.DataFrame(mongodb["orders"].find())
lineitem_df = pd.DataFrame(mongodb["lineitem"].find())

# Filter orders by date
start_date = datetime.datetime(1990, 1, 1)
end_date = datetime.datetime(1995, 1, 1)
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
filtered_orders = orders_df[(orders_df['O_ORDERDATE'] >= start_date) & (orders_df['O_ORDERDATE'] < end_date)]

# Supplier Volume Query in MongoDB
lineitem_df['revenue'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])
lineitem_agg = lineitem_df.groupby('L_ORDERKEY').agg({'revenue': 'sum'}).reset_index()

# Merge with orders table
combined_df = pd.merge(lineitem_agg, filtered_orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Merge with customers table
combined_df = pd.merge(combined_df, customers_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Redis connection and querying
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Load data as JSON string and parse it to dataframe
nation_json = json.loads(redis_client.get('nation'), object_hook=json_util.objectid_hook)
region_json = json.loads(redis_client.get('region'), object_hook=json_util.objectid_hook)
supplier_json = json.loads(redis_client.get('supplier'), object_hook=json_util.objectid_hook)

# Convert JSON to dataframe
nation_df = pd.DataFrame(nation_json)
region_df = pd.DataFrame(region_json)
supplier_df = pd.DataFrame(supplier_json)

# Get nations in ASIA region
asia_regions = region_df[region_df['R_NAME'] == 'ASIA']['R_REGIONKEY']
asia_nations = nation_df[nation_df['N_REGIONKEY'].isin(asia_regions)]

# Merge Asia suppliers with Asia nations
supplier_df = supplier_df[supplier_df['S_NATIONKEY'].isin(asia_nations['N_NATIONKEY'])]

# Filter for Asia customers and suppliers
combined_df = combined_df[combined_df['C_NATIONKEY'].isin(asia_nations['N_NATIONKEY'])]
combined_df = combined_df[combined_df['O_CUSTKEY'].isin(supplier_df['S_SUPPKEY'])]

# Group by nation, sum revenue
result_df = combined_df.groupby(['C_NATIONKEY']).agg({'revenue': 'sum'}).reset_index()

# Merge with nations table to get nation names
result_df = pd.merge(result_df, nation_df, how='left', left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Select and order the final columns
result_df = result_df[['N_NAME', 'revenue']].sort_values(by='revenue', ascending=False)

# Write to CSV
result_df.to_csv('query_output.csv', index=False)
