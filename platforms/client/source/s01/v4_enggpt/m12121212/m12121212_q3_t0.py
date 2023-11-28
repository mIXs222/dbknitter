import pymongo
import direct_redis
import pandas as pd
from datetime import datetime

# Helper function to apply discount and sum extended price
def revenue_calculation(row):
    return (row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT']))

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
orders_collection = mongo_db["orders"]

# Redis connection
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Read "orders" data from MongoDB
orders_query = {
    "O_ORDERDATE": {"$lt": datetime(1995, 3, 15)},
    "O_ORDERSTATUS": {"$in": ["F", "O", "P"]}  # Assuming we want finalized, open, or pending orders
}
mongo_orders_df = pd.DataFrame(list(orders_collection.find(orders_query, projection={"_id": False})))

# Read "customer" and "lineitem" data from Redis
customer_df = pd.read_json(redis_client.get('customer'))
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Pre-process customer data
customer_df = customer_df[customer_df['C_MKTSEGMENT'] == 'BUILDING']

# Merge customer and orders on customer key
orders_customers_df = mongo_orders_df.merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Preprocess lineitem data
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
lineitem_df = lineitem_df[lineitem_df['L_SHIPDATE'] > datetime(1995, 3, 15)]

# Merge orders with lineitem on order key
orders_customers_lineitem_df = orders_customers_df.merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Calculate revenue
orders_customers_lineitem_df['REVENUE'] = orders_customers_lineitem_df.apply(revenue_calculation, axis=1)

# Select required columns and group the data
result_df = orders_customers_lineitem_df.groupby(['O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY']).agg({'REVENUE': 'sum'}).reset_index()

# Sort by REVENUE in descending order and O_ORDERDATE in ascending order
result_df = result_df.sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True])

# Writing the results to CSV
result_df.to_csv('query_output.csv', index=False)
