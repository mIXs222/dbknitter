import pymongo
import pandas as pd
from direct_redis import DirectRedis
import csv
from datetime import datetime

# Constants
mongodb_host = 'mongodb'
mongodb_port = 27017
mongodb_db_name = 'tpch'
mongodb_collection = 'orders'
redis_host = 'redis'
redis_port = 6379
redis_tablename = 'lineitem'
date_format = '%Y-%m-%d'
start_date = datetime.strptime('1993-07-01', date_format)
end_date = datetime.strptime('1993-10-01', date_format)
output_filename = 'query_output.csv'

# Connect to MongoDB and retrieve orders within the date range
mongo_client = pymongo.MongoClient(host=mongodb_host, port=mongodb_port)
mongo_db = mongo_client[mongodb_db_name]
orders_col = mongo_db[mongodb_collection]
orders_query = {
    'O_ORDERDATE': {'$gte': start_date, '$lt': end_date}
}
orders_projection = {
    '_id': False,
    'O_ORDERKEY': True,
    'O_ORDERPRIORITY': True,
}
orders_df = pd.DataFrame(list(orders_col.find(orders_query, orders_projection)))

# Connect to Redis and retrieve lineitem data
redis_client = DirectRedis(host=redis_host, port=redis_port, db=int(redis_db_name))
lineitem_data = redis_client.get(redis_tablename)
lineitem_df = pd.read_json(lineitem_data)

# Convert string dates to datetime objects
lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])
lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])

# Filter lineitem rows where commitment date precedes receipt date
lineitem_df = lineitem_df[lineitem_df['L_COMMITDATE'] < lineitem_df['L_RECEIPTDATE']]

# Merge orders and lineitem datasets based on order keys
filtered_orders_df = pd.merge(
    orders_df,
    lineitem_df,
    left_on='O_ORDERKEY',
    right_on='L_ORDERKEY'
)

# Count orders by order priority where the condition is met
order_counts = filtered_orders_df.groupby('O_ORDERPRIORITY')['O_ORDERKEY'].nunique().reset_index()
order_counts.columns = ['O_ORDERPRIORITY', 'COUNT']

# Sort based on order priority
sorted_order_counts = order_counts.sort_values(by='O_ORDERPRIORITY')

# Write to CSV
sorted_order_counts.to_csv(output_filename, index=False)
