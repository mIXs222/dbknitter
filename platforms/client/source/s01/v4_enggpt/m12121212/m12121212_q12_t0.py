# Python code: analysis.py

from pymongo import MongoClient
from datetime import datetime
import pandas as pd
import csv

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
orders_collection = mongo_db['orders']

# Get orders from MongoDB
orders_query = {
    'O_ORDERDATE': {'$gte': datetime(1994, 1, 1), '$lte': datetime(1994, 12, 31)},
    'O_ORDERPRIORITY': {'$in': ['1-URGENT', '2-HIGH']}
}
high_priority_orders = orders_collection.find(orders_query, {'_id': 0, 'O_ORDERKEY': 1})

# Get the keys of high-priority orders
high_priority_order_keys = [order['O_ORDERKEY'] for order in high_priority_orders]

# Connect to Redis
import direct_redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get lineitem DataFrame from Redis
lineitem_df = redis_client.get('lineitem')

# Filter the lineitem DataFrame
lineitem_filtered_df = lineitem_df[
    (lineitem_df['L_SHIPMODE'].isin(['MAIL', 'SHIP'])) &
    (lineitem_df['L_COMMITDATE'] < lineitem_df['L_RECEIPTDATE']) &
    (lineitem_df['L_SHIPDATE'] < lineitem_df['L_COMMITDATE'])
]

# Split into high and low priority based on O_ORDERKEY
lineitem_high_priority_df = lineitem_filtered_df[lineitem_filtered_df['L_ORDERKEY'].isin(high_priority_order_keys)]
lineitem_low_priority_df = lineitem_filtered_df[~lineitem_filtered_df['L_ORDERKEY'].isin(high_priority_order_keys)]

# Count the number of line items in high and low priority for each shipping mode
shipping_mode_counts = {
    'SHIP_MODE': [],
    'HIGH_LINE_COUNT': [],
    'LOW_LINE_COUNT': []
}

for ship_mode in ['MAIL', 'SHIP']:
    shipping_mode_counts['SHIP_MODE'].append(ship_mode)
    shipping_mode_counts['HIGH_LINE_COUNT'].append(len(lineitem_high_priority_df[lineitem_high_priority_df['L_SHIPMODE'] == ship_mode]))
    shipping_mode_counts['LOW_LINE_COUNT'].append(len(lineitem_low_priority_df[lineitem_low_priority_df['L_SHIPMODE'] == ship_mode]))

# Create a DataFrame from the results
result_df = pd.DataFrame(shipping_mode_counts)

# Sort the DataFrame by shipping mode
result_df.sort_values(by=['SHIP_MODE'], inplace=True)

# Write to CSV
result_df.to_csv('query_output.csv', index=False)

# Close clients
mongo_client.close()
redis_client.close()
