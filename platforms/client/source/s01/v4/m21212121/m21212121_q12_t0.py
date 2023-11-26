import pymongo
from pymongo import MongoClient
import pandas as pd
from direct_redis import DirectRedis
import csv

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
mongodb = client['tpch']
lineitem_collection = mongodb['lineitem']

# Retrieve lineitem data from MongoDB
lineitem_query = {
    'L_SHIPMODE': {'$in': ['MAIL', 'SHIP']},
    'L_COMMITDATE': {'$lt': '$L_RECEIPTDATE'},
    'L_SHIPDATE': {'$lt': '$L_COMMITDATE'},
    'L_RECEIPTDATE': {'$gte': '1994-01-01', '$lt': '1995-01-01'}
}
lineitem_projection = {
    'L_ORDERKEY': 1,
    'L_SHIPMODE': 1
}
lineitem_df = pd.DataFrame(list(lineitem_collection.find(lineitem_query, lineitem_projection)))

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379)

# Retrieve orders data from Redis and load into DataFrame
orders_str = redis_client.get('orders')
orders_df = pd.read_json(orders_str)

# Merge dataframes on O_ORDERKEY = L_ORDERKEY
merged_df = pd.merge(orders_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Apply the conditions for the URGENT and HIGH counts
merged_df['HIGH_LINE_COUNT'] = merged_df['O_ORDERPRIORITY'].apply(lambda x: 1 if x in ['1-URGENT', '2-HIGH'] else 0)
merged_df['LOW_LINE_COUNT'] = merged_df['O_ORDERPRIORITY'].apply(lambda x: 1 if x not in ['1-URGENT', '2-HIGH'] else 0)

# Group by L_SHIPMODE and calculate sums
grouped_df = merged_df.groupby('L_SHIPMODE').agg({
    'HIGH_LINE_COUNT': 'sum',
    'LOW_LINE_COUNT': 'sum'
}).reset_index()

# Write to CSV
grouped_df.to_csv('query_output.csv', index=False)
