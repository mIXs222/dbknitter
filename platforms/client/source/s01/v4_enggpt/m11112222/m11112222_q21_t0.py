# mongo_queries.py

from pymongo import MongoClient
import pandas as pd
import redis
from direct_redis import DirectRedis
import csv

# MongoDB connection
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Redis connection
r = DirectRedis(host='redis', port=6379, db=0)

# DirectRedis client wrapper to read Pandas DataFrame
def read_redis_df(key):
    data = r.get(key)
    if data:
        return pd.read_json(data)
    else:
        return pd.DataFrame()

# Fetch tables from mongodb
supplier_df = pd.DataFrame(list(mongo_db.supplier.find({"S_NATIONKEY": {"$in": list(mongo_db.nation.find({"N_NAME": "SAUDI ARABIA"}, {"N_NATIONKEY": 1}))}})))
nation_df = pd.DataFrame(list(mongo_db.nation.find()))

# Fetch tables from redis
orders_df = read_redis_df('orders')
lineitem_df = read_redis_df('lineitem')

# Filter orders with order status 'F'
orders_df = orders_df[orders_df['O_ORDERSTATUS'] == 'F']

# Join tables and perform filters
result = (
    supplier_df.merge(lineitem_df, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
    .merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    .merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
)

# Further filter based on commit and receipt dates
result = result[result['L_RECEIPTDATE'] > result['L_COMMITDATE']]

# Group by suppliers, count waiting times and sort results
result = result.groupby('S_NAME').agg(NUMWAIT=('L_ORDERKEY', 'count')).reset_index()
result = result.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Output to CSV
result.to_csv('query_output.csv', index=False)
