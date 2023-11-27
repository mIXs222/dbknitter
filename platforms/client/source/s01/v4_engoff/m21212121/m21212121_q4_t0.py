import pymongo
import pandas as pd
from direct_redis import DirectRedis
import csv
from datetime import datetime

# Connection details
mongodb_connection = {
    'host': 'mongodb',
    'port': 27017,
    'db_name': 'tpch'
}

redis_connection = {
    'host': 'redis',
    'port': 6379,
    'db_name': 0
}

# Connect to MongoDB
client = pymongo.MongoClient(host=mongodb_connection['host'], port=mongodb_connection['port'])
mongodb = client[mongodb_connection['db_name']]

# Connect to Redis
redis_client = DirectRedis(host=redis_connection['host'], port=redis_connection['port'])

# Fetch MongoDB data
lineitem_df = pd.DataFrame(list(mongodb.lineitem.find(
    {
        "L_COMMITDATE": {"$lt": "L_RECEIPTDATE"},
        "L_RECEIPTDATE": {"$gte": datetime(1993, 7, 1), "$lt": datetime(1993, 10, 1)}
    },
    {
        "L_ORDERKEY": 1,
    }
)))

# Fetch Redis data
orders_str = redis_client.get('orders')
orders_df = pd.read_csv(pd.compat.StringIO(orders_str.decode("utf-8")))

# Merge data from MongoDB and Redis on their respective order key columns
merged_df = orders_df[orders_df['O_ORDERKEY'].isin(lineitem_df['L_ORDERKEY'])]

# Group by order priority and count the number of orders
result_df = merged_df.groupby('O_ORDERPRIORITY').size().reset_index(name='order_count')

# Sort the DataFrame by order priority
result_df.sort_values(by='O_ORDERPRIORITY', ascending=True, inplace=True)

# Write the output to a CSV file
result_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
