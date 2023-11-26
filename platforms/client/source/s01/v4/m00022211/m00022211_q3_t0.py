# query.py
import pymongo
import pandas as pd
from datetime import datetime
import direct_redis

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
orders_coll = mongo_db["orders"]
lineitem_coll = mongo_db["lineitem"]

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve customer data from Redis
customer_df = r.get('customer')
customer_df = pd.read_msgpack(customer_df)

# Convert Redis data to DataFrame
customer_df = pd.DataFrame(customer_df)

# Filter customers with MARKETSEGMENT 'BUILDING'
customer_df = customer_df[customer_df['C_MKTSEGMENT'] == 'BUILDING']

# Retrieve orders and lineitem data from MongoDB
orders_query = {"O_ORDERDATE": {"$lt": datetime(1995, 3, 15)}}
orders_df = pd.DataFrame(list(orders_coll.find(orders_query)))

lineitem_query = {"L_SHIPDATE": {"$gt": datetime(1995, 3, 15)}}
lineitem_df = pd.DataFrame(list(lineitem_coll.find(lineitem_query)))

# Merge DataFrames on customer key and order key
merged_df = pd.merge(customer_df, orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = pd.merge(merged_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Select needed columns and perform calculations
result_df = merged_df.groupby(['L_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY']).agg({
    'REVENUE': pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc=lambda x: (x * (1 - merged_df.loc[x.index, 'L_DISCOUNT'])).sum())
}).reset_index()

# Order the result
result_df = result_df.sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True])

# Save the result to CSV
result_df.to_csv('query_output.csv', index=False)
