import pymongo
import pandas as pd
from direct_redis import DirectRedis
import csv
import datetime

# Connecting to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
lineitem_collection = mongo_db["lineitem"]

# Querying MongoDB for lineitem data
lineitem_query = {
    "L_SHIPMODE": {"$in": ["MAIL", "SHIP"]},
    "L_RECEIPTDATE": {"$gte": datetime.datetime(1994, 1, 1), "$lt": datetime.datetime(1995, 1, 1)},
    "L_SHIPDATE": {"$lt": "$L_COMMITDATE"}
}
lineitem_projection = {
    "_id": 0,
    "L_ORDERKEY": 1,
    "L_RECEIPTDATE": 1,
    "L_COMMITDATE": 1,
    "L_SHIPMODE": 1
}
lineitem_df = pd.DataFrame(list(lineitem_collection.find(lineitem_query, lineitem_projection)))

# Connecting to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Querying Redis for orders data
orders_str = redis_client.get('orders')
orders_df = pd.read_json(orders_str)

# Merge lineitem and orders data
merged_df = lineitem_df.merge(orders_df, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Determine late lineitems
merged_df['is_late'] = merged_df['L_RECEIPTDATE'] > merged_df['L_COMMITDATE']

# Partition by priority
urgent_high_priority = merged_df[(merged_df['O_ORDERPRIORITY'] == 'URGENT') | (merged_df['O_ORDERPRIORITY'] == 'HIGH')]
other_priority = merged_df[(merged_df['O_ORDERPRIORITY'] != 'URGENT') & (merged_df['O_ORDERPRIORITY'] != 'HIGH')]

# Count late lineitems by shipmode and priority
result_df = pd.DataFrame({
    'Ship_Mode': ['MAIL', 'SHIP'],
    'URGENT/HIGH_Priority_Late': [
        urgent_high_priority[urgent_high_priority['L_SHIPMODE'] == 'MAIL']['is_late'].sum(),
        urgent_high_priority[urgent_high_priority['L_SHIPMODE'] == 'SHIP']['is_late'].sum(),
    ],
    'Other_Priority_Late': [
        other_priority[other_priority['L_SHIPMODE'] == 'MAIL']['is_late'].sum(),
        other_priority[other_priority['L_SHIPMODE'] == 'SHIP']['is_late'].sum(),
    ]
})

# Write the result to a csv file
result_df.to_csv('query_output.csv', index=False)
