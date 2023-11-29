# query.py
from pymongo import MongoClient
from datetime import datetime
import pandas as pd
import direct_redis
import csv

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_nation = mongo_db['nation']
mongo_orders = mongo_db['orders']

# Get relevant data from MongoDB: nations and qualifying orders
nations_df = pd.DataFrame(list(mongo_nation.find({}, {'_id': 0})))
orders_df = pd.DataFrame(list(mongo_orders.find(
    {"O_ORDERDATE": {"$gte": datetime(1993, 10, 1), "$lt": datetime(1994, 1, 1)}},
    {'_id': 0}
)))

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get relevant data from Redis: customers and lineitems
customer_df = pd.read_json(redis_client.get('customer'))
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Merge dataframes
merged_df = lineitem_df.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merged_df = merged_df.merge(customer_df, left_on='L_ORDERKEY', right_on='C_CUSTKEY')

# Calculate revenue lost
merged_df['REVENUE_LOST'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Group by customer, sum revenue lost and sort as required
result_df = merged_df.groupby(['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']) \
    .agg(REVENUE_LOST=('REVENUE_LOST', 'sum')) \
    .reset_index()

result_df = result_df.sort_values(by=['REVENUE_LOST', 'C_CUSTKEY', 'C_NAME'], ascending=[True, True, True])
result_df = result_df.sort_values(by='C_ACCTBAL', ascending=False)

# Merge with nations to get the nation name
result_df = result_df.merge(nations_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
result_df['NATION'] = result_df['N_NAME']
result_df = result_df.drop(columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])

# Selecting the final columns and writing the output to a CSV file
final_df = result_df[['C_CUSTKEY', 'C_NAME', 'REVENUE_LOST', 'C_ACCTBAL', 'NATION', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']]
final_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_ALL)
