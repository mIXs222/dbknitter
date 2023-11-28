# Import libraries
import pymongo
from pymongo import MongoClient
import pandas as pd
from direct_redis import DirectRedis
import re

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_collection = mongo_db['customer']

# Fetch MongoDB data
cursor = mongo_collection.find({}, {'_id': 0})
customer_df = pd.DataFrame(list(cursor))

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Fetch Redis data
orders_data = redis_client.get('orders')
orders_df = pd.read_json(orders_data)
# Filter out orders with 'pending' or 'deposits' in the comments
orders_df_filtered = orders_df[~orders_df['O_COMMENT'].str.contains('pending|deposits', flags=re.IGNORECASE, regex=True)]

# Combine the data
merged_df = pd.merge(customer_df, orders_df_filtered, how='left', left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# Perform the analysis
merged_df['C_COUNT'] = merged_df.groupby('C_CUSTKEY')['O_ORDERKEY'].transform('count')
result = merged_df.groupby('C_COUNT').agg(CUSTDIST=('C_CUSTKEY', 'nunique')).reset_index()
result = result.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False])

# Output to CSV
result.to_csv('query_output.csv', index=False)
