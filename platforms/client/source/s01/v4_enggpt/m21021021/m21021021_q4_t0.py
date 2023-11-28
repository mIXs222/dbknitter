# query.py

import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# Connection to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
lineitem_collection = mongo_db["lineitem"]

# Connection to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Define the timeframe
start_date = datetime(1993, 7, 1)
end_date = datetime(1993, 10, 1)

# Query MongoDB for lineitem details
lineitem_query = {
    'L_COMMITDATE': {'$lt': 'L_RECEIPTDATE'},
    'L_SHIPDATE': {'$gte': start_date, '$lte': end_date}
}
lineitem_projection = {
    '_id': False,
    'L_ORDERKEY': True
}
lineitem_df = pd.DataFrame(list(lineitem_collection.find(lineitem_query, lineitem_projection)))

# Retrieve orders data from Redis
orders_str = redis_client.get('orders')
orders_df = pd.read_json(orders_str)
orders_df = orders_df.rename(columns=lambda x: x[2:])

# Merge and filter the orders and lineitem data
merged_df = orders_df.merge(lineitem_df, how='inner', left_on='ORDERKEY', right_on='L_ORDERKEY')

# Filter orders based on the given timeframe
filtered_orders = merged_df[
    (merged_df['ORDERDATE'] >= start_date) & \
    (merged_df['ORDERDATE'] <= end_date)
]

# Group orders by order priority and count
order_priorities_count = filtered_orders.groupby('ORDERPRIORITY')['ORDERKEY'].nunique().reset_index(name='COUNT')

# Sort by order priority
order_priorities_count_sorted = order_priorities_count.sort_values(by='ORDERPRIORITY')

# Save to CSV
order_priorities_count_sorted.to_csv('query_output.csv', index=False)
