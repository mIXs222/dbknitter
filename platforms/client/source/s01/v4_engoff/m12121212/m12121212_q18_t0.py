import pandas as pd
from pymongo import MongoClient
import direct_redis
import csv

# Constants for the quantity threshold
QUANTITY_THRESHOLD = 300

# Connection details
MONGODB_DETAILS = {
    'hostname': 'mongodb',
    'port': 27017,
    'database': 'tpch'
}

REDIS_DETAILS = {
    'hostname': 'redis',
    'port': 6379,
    'database': 0
}

# Connect to MongoDB
mongo_client = MongoClient(host=MONGODB_DETAILS['hostname'], port=MONGODB_DETAILS['port'])
mongo_db = mongo_client[MONGODB_DETAILS['database']]
orders_collection = mongo_db['orders']

# Connect to Redis
redis_client = direct_redis.DirectRedis(host=REDIS_DETAILS['hostname'], port=REDIS_DETAILS['port'], db=REDIS_DETAILS['database'])
redis_customer_string = redis_client.get('customer')
redis_lineitem_string = redis_client.get('lineitem')

# DataFrame from Redis strings
customer_df = pd.read_json(redis_customer_string)
lineitem_df = pd.read_json(redis_lineitem_string)

# First, filter lineitems for those with quantity > 300
large_volume_lineitems = lineitem_df[lineitem_df['L_QUANTITY'] > QUANTITY_THRESHOLD]

# Now, find the orders from MongoDB that match these lineitems
large_volume_order_keys = large_volume_lineitems['L_ORDERKEY'].unique().tolist()
large_volume_orders_cursor = orders_collection.find(
    {
        'O_ORDERKEY': {'$in': large_volume_order_keys}
    },
    {
        'O_ORDERKEY': 1,
        'O_CUSTKEY': 1,
        'O_ORDERDATE': 1,
        'O_TOTALPRICE': 1,
        '_id': 0
    }
)

# Convert cursor to DataFrame
large_volume_orders_df = pd.DataFrame(list(large_volume_orders_cursor))

# Merge DataFrames to get result
result = pd.merge(
    large_volume_orders_df,
    customer_df,
    how="left",
    left_on="O_CUSTKEY",
    right_on="C_CUSTKEY"
)[['C_NAME', 'O_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'L_QUANTITY']]

# Save the resulting DataFrame to a CSV file
result.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_ALL)
