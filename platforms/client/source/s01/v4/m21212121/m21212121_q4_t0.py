import pymongo
from bson import Code
import direct_redis
import pandas as pd

# Connect to MongoDB
client = pymongo.MongoClient("mongodb", 27017)
mongo_db = client['tpch']
lineitem = mongo_db['lineitem']

# Aggregation Framework for MongoDB to filter lineitem entries
pipeline = [
    {'$match': {
        'L_COMMITDATE': {'$lt': '$L_RECEIPTDATE'}
    }},
    {'$group': {
        '_id': '$L_ORDERKEY'
    }}
]

# Execute aggregation to get valid L_ORDERKEYs
valid_orderkeys = set(doc['_id'] for doc in lineitem.aggregate(pipeline))

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Read 'orders' data into a pandas DataFrame
orders_str = r.get('orders')
orders_df = pd.read_json(orders_str)

# Filter based on the valid_orderkeys from MongoDB
filtered_orders = orders_df[orders_df['O_ORDERKEY'].isin(valid_orderkeys)]

# Further filter based on order date
filtered_orders = filtered_orders[
    (filtered_orders['O_ORDERDATE'] >= '1993-07-01') &
    (filtered_orders['O_ORDERDATE'] < '1993-10-01')
]

# Group and count order priorities
order_counts = filtered_orders.groupby('O_ORDERPRIORITY').size().reset_index(name='ORDER_COUNT')

# Order the results based on O_ORDERPRIORITY
order_counts = order_counts.sort_values(by='O_ORDERPRIORITY')

# Write the results to CSV
order_counts.to_csv('query_output.csv', index=False)
