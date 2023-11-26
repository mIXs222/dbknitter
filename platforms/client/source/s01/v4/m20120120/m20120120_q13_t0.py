# Filename: execute_query.py
import pymongo
from direct_redis import DirectRedis
import pandas as pd

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
customer_collection = mongo_db["customer"]

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Fetch customer data from MongoDB
customer_data = list(customer_collection.find({}, {'_id': 0}))
df_customer = pd.DataFrame(customer_data)

# Fetch orders data from Redis
orders_str = redis_client.get('orders')
df_orders = pd.read_json(orders_str)

# Perform the LEFT JOIN operation
merged_df = pd.merge(df_customer, df_orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='left')

# Filter based on orders comment
merged_df = merged_df[~merged_df['O_COMMENT'].astype(str).str.contains('pending%deposits%', na=False)]

# Calculate C_COUNT
grouped = merged_df.groupby('C_CUSTKEY').agg(C_COUNT=('O_ORDERKEY', 'count')).reset_index()

# Calculate CUSTDIST
result = grouped.groupby('C_COUNT').agg(CUSTDIST=('C_COUNT', 'count')).reset_index()

# Order the results
result = result.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False])

# Write result to query_output.csv
result.to_csv('query_output.csv', index=False)
