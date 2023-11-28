import pymongo
import pandas as pd
from datetime import datetime
import direct_redis
import csv

# Connect to MongoDB
client = pymongo.MongoClient("mongodb", 27017)
mongodb = client["tpch"]

# Fetch orders from MongoDB within the specified timeframe
start_date = datetime.strptime('1993-07-01', '%Y-%m-%d')
end_date = datetime.strptime('1993-10-01', '%Y-%m-%d')
mongo_orders = mongodb.orders.find({
    "O_ORDERDATE": {
        "$gte": start_date,
        "$lt": end_date
    }
}, {'_id': 0})

# Convert MongoDB orders to DataFrame
orders_df = pd.DataFrame(list(mongo_orders))

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch lineitem from Redis and convert to DataFrame
lineitem_data = redis_client.get('lineitem')
lineitem_df = pd.read_json(lineitem_data)

# Merge orders and lineitems on O_ORDERKEY and L_ORDERKEY
merged_df = pd.merge(orders_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Filter orders with commitment date before the receipt date
filtered_df = merged_df[merged_df['L_COMMITDATE'] < merged_df['L_RECEIPTDATE']]

# Calculate the count of orders for each priority that meets the conditions
priority_counts = filtered_df.groupby('O_ORDERPRIORITY').size().reset_index(name='count')

# Sort by order priority
sorted_counts = priority_counts.sort_values(by='O_ORDERPRIORITY')

# Write to CSV
sorted_counts.to_csv('query_output.csv', index=False)
