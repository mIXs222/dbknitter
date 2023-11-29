import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
mongo_orders_col = mongo_db["orders"]

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Query MongoDB for orders with high priority
orders_high_priority = list(mongo_orders_col.find({
    "O_ORDERPRIORITY": {"$in": ["URGENT", "HIGH"]}
}, {"_id": 0, "O_ORDERKEY": 1}))

orders_high_priority_keys = [doc["O_ORDERKEY"] for doc in orders_high_priority]

# Load lineitems from Redis into a DataFrame
lineitem_df = pd.read_json(redis_client.get('lineitem'), orient='records')

# Parse string dates to datetime
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])
lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])

# Filter lineitem data based on the conditions specified in the query
filtered_lineitem_df = lineitem_df[
    (lineitem_df['L_SHIPMODE'].isin(['mail', 'ship'])) &
    (lineitem_df['L_RECEIPTDATE'] >= datetime(1994, 1, 1)) &
    (lineitem_df['L_RECEIPTDATE'] < datetime(1995, 1, 1)) &
    (lineitem_df['L_RECEIPTDATE'] > lineitem_df['L_COMMITDATE']) &
    (lineitem_df['L_SHIPDATE'] < lineitem_df['L_COMMITDATE'])
]

# Count high and low priority lineitems after merging with orders high priority
filtered_lineitem_df['HIGH_PRIORITY'] = filtered_lineitem_df['L_ORDERKEY'].isin(orders_high_priority_keys)
filtered_lineitem_df['LOW_PRIORITY'] = ~filtered_lineitem_df['HIGH_PRIORITY']

# Group by L_SHIPMODE and count
result_df = filtered_lineitem_df.groupby('L_SHIPMODE').agg({
    'HIGH_PRIORITY': 'sum',
    'LOW_PRIORITY': 'sum'
}).reset_index()

result_df.rename(columns={'HIGH_PRIORITY': 'HIGH_PRIORITY_COUNT', 'LOW_PRIORITY': 'LOW_PRIORITY_COUNT'}, inplace=True)

# Sort by ship mode
result_df.sort_values(by='L_SHIPMODE', ascending=True, inplace=True)

# Write output to CSV
result_df.to_csv('query_output.csv', index=False)
