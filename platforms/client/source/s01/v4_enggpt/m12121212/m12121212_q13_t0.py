# Save this code as query.py
import pymongo
from direct_redis import DirectRedis
import pandas as pd
import re

# Connection to MongoDB
mongodb_client = pymongo.MongoClient("mongodb", 27017)
mongodb_db = mongodb_client["tpch"]

# Retrieve 'orders' collection
orders_collection = mongodb_db["orders"]
orders_data = list(orders_collection.find({}, {
    "_id": 0, "O_ORDERKEY": 1, "O_CUSTKEY": 1, "O_COMMENT": 1
}))

# Filtering orders with comments not containing 'pending' or 'deposits'
orders = [order for order in orders_data if not re.search("pending|deposits", order["O_COMMENT"], re.IGNORECASE)]

# Convert to DataFrame
orders_df = pd.DataFrame(orders).rename(columns={'O_ORDERKEY': 'ORDERKEY', 'O_CUSTKEY': 'CUSTKEY'})

# Connection to Redis
redis_client = DirectRedis(host="redis", port=6379, db=0)

# Retrieve 'customer' data
customer_data = redis_client.get('customer')
customer_df = pd.read_json(customer_data).rename(columns={'C_CUSTKEY': 'CUSTKEY'})

# Left join operation
combined_df = customer_df.merge(orders_df, how='left', on='CUSTKEY')

# Count of orders per customer
combined_df['C_COUNT'] = combined_df.groupby('CUSTKEY')['ORDERKEY'].transform('count')

# Count of customers per order count
custdist_df = combined_df.groupby('C_COUNT').agg({'CUSTKEY': 'nunique'}).reset_index()
custdist_df.columns = ['C_COUNT', 'CUSTDIST']

# Ordering the results
custdist_df.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False], inplace=True)

# Write results to CSV
custdist_df.to_csv('query_output.csv', index=False)
