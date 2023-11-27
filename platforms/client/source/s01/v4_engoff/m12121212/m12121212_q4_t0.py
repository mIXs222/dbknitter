import pymongo
import pandas as pd
import direct_redis
import csv

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
orders_collection = mongo_db["orders"]

# Select orders from MongoDB within the date range
start_date = "1993-07-01"
end_date = "1993-10-01"
orders = orders_collection.find({
    "O_ORDERDATE": {"$gte": start_date, "$lt": end_date}
})
orders_df = pd.DataFrame(list(orders))

# Connect to Redis
redis_client = direct_redis.DirectRedis(host="redis", port=6379, db=0)
lineitem_df = pd.DataFrame(eval(redis_client.get('lineitem')))

# Convert string dates to datetime objects for comparisons
lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])
lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])

# Find lineitems where the receive date is after the commit date and get those orders' IDs
late_lineitems = lineitem_df[
    lineitem_df['L_RECEIPTDATE'] > lineitem_df['L_COMMITDATE']
]
late_order_keys = set(late_lineitems['L_ORDERKEY'])

# Filter orders_df for orders that contain late lineitems
late_orders_df = orders_df[orders_df['O_ORDERKEY'].isin(late_order_keys)]

# Calculate the number of late orders for each order priority
order_priority_counts = late_orders_df.groupby('O_ORDERPRIORITY').size().reset_index(name='order_count')

# Sort by order priority
order_priority_counts_sorted = order_priority_counts.sort_values(by=['O_ORDERPRIORITY'])

# Convert the result to CSV and save it
order_priority_counts_sorted.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_ALL)
