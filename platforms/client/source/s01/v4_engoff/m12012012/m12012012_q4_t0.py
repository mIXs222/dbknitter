from pymongo import MongoClient
from direct_redis import DirectRedis
import pandas as pd
from datetime import datetime
import csv

# Install PyMongo and DirectRedis if necessary.

# Connect to MongoDB
client = MongoClient(host='mongodb', port=27017)
mongodb = client['tpch']
orders_collection = mongodb['orders']

# Connect to Redis
redis = DirectRedis(host='redis', port=6379)
redis_select_db = redis.command('SELECT 0')

# Query MongoDB for orders between the specified dates
start_date = datetime(1993, 7, 1)
end_date = datetime(1993, 10, 1)
orders_query = {
    'O_ORDERDATE': {'$gte': start_date, '$lt': end_date},
}
orders_projection = {
    'O_ORDERKEY': 1,
    'O_ORDERPRIORITY': 1,
}

orders_data = list(orders_collection.find(orders_query, orders_projection))

# Parse the lineitem table from Redis
lineitem_dataframe = pd.read_json(redis.get('lineitem'))
lineitem_dataframe['L_COMMITDATE'] = pd.to_datetime(lineitem_dataframe['L_COMMITDATE'])
lineitem_dataframe['L_RECEIPTDATE'] = pd.to_datetime(lineitem_dataframe['L_RECEIPTDATE'])

# Combine the data from both databases
orders_df = pd.DataFrame(orders_data)
orders_df = orders_df.rename(columns={'O_ORDERKEY': 'L_ORDERKEY', 'O_ORDERPRIORITY': 'ORDER_PRIORITY'})

# Filter lineitems that were received by the customer after the commit date
late_lineitems = lineitem_dataframe[lineitem_dataframe['L_COMMITDATE'] < lineitem_dataframe['L_RECEIPTDATE']]

# Join on order key
result_df = pd.merge(orders_df, late_lineitems, on='L_ORDERKEY', how='inner')

# Count the number of such orders for each order priority
priority_count = result_df.groupby('ORDER_PRIORITY').size().reset_index(name='NUM_OF_ORDERS')

# Sort by order priority
priority_count_sorted = priority_count.sort_values(by='ORDER_PRIORITY')

# Write the output to a csv file
priority_count_sorted.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
