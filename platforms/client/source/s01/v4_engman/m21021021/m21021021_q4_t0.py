import pymongo
from bson import json_util
import pandas as pd
import json
from datetime import datetime
import direct_redis

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = mongo_client["tpch"]
lineitem_collection = mongodb["lineitem"]

# Generate the query for MongoDB
pipeline = [
    {"$match": {
        "L_SHIPDATE": {"$gte": datetime(1993, 7, 1), "$lt": datetime(1993, 10, 1)},
        "L_COMMITDATE": {"$lt": "L_RECEIPTDATE"}
    }},
    {"$project": {"L_ORDERKEY": 1}}
]
mongo_lineitems = list(lineitem_collection.aggregate(pipeline))

# Extract order keys
order_keys = [lineitem["L_ORDERKEY"] for lineitem in mongo_lineitems]

# Redis connection and query
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
orders_json_list = r.get('orders')
orders_df = pd.read_json(orders_json_list, orient='split')

# Filter orders that are in the MongoDB result
filtered_orders_df = orders_df[orders_df['O_ORDERKEY'].isin(order_keys)]

# Group by O_ORDERPRIORITY and count
order_priority_count = filtered_orders_df.groupby(["O_ORDERPRIORITY"])["O_ORDERKEY"].count().reset_index(name='ORDER_COUNT')

# Sort by O_ORDERPRIORITY
sorted_order_priority_count = order_priority_count.sort_values(by="O_ORDERPRIORITY")

# Save query result to file
sorted_order_priority_count.to_csv('query_output.csv', index=False)
