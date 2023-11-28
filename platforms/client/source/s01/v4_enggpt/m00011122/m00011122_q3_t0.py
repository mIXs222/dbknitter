# import required libraries
import pymongo
import pandas as pd
import redis
from datetime import datetime
import csv

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
mongo_customers_collection = mongo_db["customer"]

# Connect to Redis
redis_client = redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)

# Filter customers with 'BUILDING' market segment in MongoDB
building_customers_df = pd.DataFrame(list(mongo_customers_collection.find({"C_MKTSEGMENT": "BUILDING"}, {'_id': 0})))

# Get 'orders' and 'lineitem' tables from Redis and convert them to Pandas DataFrames
orders_table = pd.read_json(redis_client.get('orders'))
lineitem_table = pd.read_json(redis_client.get('lineitem'))

# Filter orders placed before March 15, 1995, and lineitems shipped after March 15, 1995
filtered_orders = orders_table[(orders_table['O_ORDERDATE'] < "1995-03-15")]
filtered_lineitems = lineitem_table[(lineitem_table['L_SHIPDATE'] > "1995-03-15")]

# Merge all the dataframes into one
merged_df = building_customers_df.merge(filtered_orders, how='inner', left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = merged_df.merge(filtered_lineitems, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Calculate the revenue
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Group by order key, order date, and shipping priority
final_df = merged_df.groupby(['O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'])['REVENUE'].sum().reset_index()

# Order the results
final_df.sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True], inplace=True)

# Write the result to 'query_output.csv'
final_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
