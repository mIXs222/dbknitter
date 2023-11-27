import pymongo
import pandas as pd
from datetime import datetime
from decimal import Decimal
import direct_redis

# Set up connection details
MONGO_DB = "tpch"
MONGO_HOST = "mongodb"
MONGO_PORT = 27017
REDIS_DB = 0
REDIS_HOST = "redis"
REDIS_PORT = 6379

# Establish connection to MongoDB
mongo_client = pymongo.MongoClient(host=MONGO_HOST, port=MONGO_PORT)
mongo_tpch = mongo_client[MONGO_DB]

# Load data from MongoDB
nation_df = pd.DataFrame(list(mongo_tpch.nation.find()))
region_df = pd.DataFrame(list(mongo_tpch.region.find()))
supplier_df = pd.DataFrame(list(mongo_tpch.supplier.find()))

# Filter Asian nations
asian_region = region_df[region_df['R_NAME'] == 'ASIA']
asian_nations = nation_df[nation_df['N_REGIONKEY'].isin(asian_region['R_REGIONKEY'])]

# Prepare Redis connection
redis_client = direct_redis.DirectRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

# Load data from Redis
customer_df = pd.read_msgpack(redis_client.get('customer'))
orders_df = pd.read_msgpack(redis_client.get('orders'))
lineitem_df = pd.read_msgpack(redis_client.get('lineitem'))

# Convert datetime strings to datetime objects and filter date range
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
date_start = datetime(1990, 1, 1)
date_end = datetime(1995, 1, 1)
filtered_orders = orders_df[(orders_df['O_ORDERDATE'] >= date_start) & (orders_df['O_ORDERDATE'] < date_end)]

# Merge dataframes to filter relevant lineitem transactions
merged_df = (lineitem_df[lineitem_df['L_ORDERKEY'].isin(filtered_orders['O_ORDERKEY'])]
             .merge(filtered_orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
             .merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
             .merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
             .merge(asian_nations, left_on='C_NATIONKEY', right_on='N_NATIONKEY'))

# Calculate revenue volume
merged_df['revenue'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
revenue_volume = (merged_df.groupby('N_NAME')
                  .agg({'revenue': 'sum'})
                  .reset_index()
                  .sort_values('revenue', ascending=False))

# Write output to CSV
revenue_volume.to_csv('query_output.csv', index=False)
