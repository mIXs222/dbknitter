from pymongo import MongoClient
import direct_redis
import pandas as pd
import datetime

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
lineitems = mongo_db['lineitem']

# Query lineitems from MongoDB for dates of interest
query = {
    "L_RECEIPTDATE": {"$gt": datetime.datetime(1993, 7, 1), "$lte": datetime.datetime(1993, 10, 1)},
    "L_COMMITDATE": {"$lt": "$L_RECEIPTDATE"}
}
projection = {
    "_id": 0,
    "L_ORDERKEY": 1
}
lineitem_orders = lineitems.find(query, projection)

# Create a DataFrame for lineitem orders
lineitem_df = pd.DataFrame(list(lineitem_orders))

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve orders data from Redis
orders_data = r.get('orders')
orders_df = pd.read_json(orders_data)

# Convert to DataFrame
orders_df = pd.read_json(orders_data)

# Merge orders and lineitems DataFrames based on L_ORDERKEY (MongoDB) and O_ORDERKEY (Redis)
merged_df = pd.merge(orders_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY', how='inner')

# Filter merged dataframe for order date in the specific range
filtered_df = merged_df[(merged_df['O_ORDERDATE'] >= pd.Timestamp('1993-07-01')) &
                        (merged_df['O_ORDERDATE'] <= pd.Timestamp('1993-10-01'))]

# Get the count of such orders for each order priority
result = filtered_df.groupby('O_ORDERPRIORITY').size().reset_index(name='order_count')

# Sort by order priority
result_sorted = result.sort_values(by='O_ORDERPRIORITY')

# Write output to CSV
result_sorted.to_csv('query_output.csv', index=False)

# Clean up database connections
mongo_client.close()
