# file: execute_query.py

import pymongo
from bson import json_util
import redis
import pandas as pd

# Connect to MongoDB
client = pymongo.MongoClient('mongodb', 27017)
mongodb = client['tpch']

# Connect to Redis
class DirectRedis(redis.Redis):
    pass

redis_client = DirectRedis(host='redis', port=6379, db=0, decode_responses=True)

# Fetch data from MongoDB
lineitem = pd.DataFrame(list(mongodb.lineitem.find({'L_QUANTITY': {'$gt': 300}}, {'_id': 0})))

# Since the customer might not necessarily have required fields in an order, we just fetch all customers for now
customers = pd.DataFrame(list(mongodb.customer.find({}, {'_id': 0})))

# Fetch data from Redis
orders_df = pd.DataFrame()
orders = redis_client.get('orders')
if orders:
    orders_df = pd.read_json(orders, orient='index')

# Merge dataframes to get the desired result
result = pd.merge(left=customers, right=lineitem, how='inner', left_on='C_CUSTKEY', right_on='L_ORDERKEY')
result = pd.merge(left=result, right=orders_df, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Filtering columns to match the query specification
result_filtered = result[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'L_QUANTITY']]

# Write the output to csv
result_filtered.to_csv('query_output.csv', index=False)
