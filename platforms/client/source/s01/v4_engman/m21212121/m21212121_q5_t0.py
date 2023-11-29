import pymongo
import pandas as pd
from bson.son import SON
from direct_redis import DirectRedis
import datetime

# MongoDB Connection
mongo_client = pymongo.MongoClient('mongodb://mongodb:27017/')
mongo_db = mongo_client['tpch']

# Redis Connection
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Load data from MongoDB
region = pd.DataFrame(list(mongo_db.region.find()))
nation = pd.DataFrame(list(mongo_db.nation.find()))
supplier = pd.DataFrame(list(mongo_db.supplier.find()))
customer = pd.DataFrame(list(mongo_db.customer.find()))
lineitem = pd.DataFrame(list(mongo_db.lineitem.find()))

# Load data from Redis
nation_redis = pd.read_json(redis_client.get('nation'))
orders_redis = pd.read_json(redis_client.get('orders'))

# Filter region for ASIA and merge with nation
asia_region = region[region.R_NAME == 'ASIA']
nation = nation.merge(asia_region, left_on='N_REGIONKEY', right_on='R_REGIONKEY')

# Convert the date column from string to datetime in both MongoDB and Redis orders data
orders_redis['O_ORDERDATE'] = pd.to_datetime(orders_redis['O_ORDERDATE'])
min_date = datetime.datetime(1990, 1, 1)
max_date = datetime.datetime(1995, 1, 1)

# Filter orders by date range
orders_redis = orders_redis[(orders_redis['O_ORDERDATE'] >= min_date) & (orders_redis['O_ORDERDATE'] <= max_date)]

# Merge suppliers with nations to filter suppliers within ASIA
supplier = supplier.merge(nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Merge customers with nations to filter customers within ASIA
customer = customer.merge(nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Merge lineitems with orders
lineitem = lineitem.merge(orders_redis, left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Merge lineitems with filtered suppliers
lineitem = lineitem.merge(supplier, left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Merge lineitems with filtered customers
lineitem = lineitem.merge(customer, left_on='L_SUPPKEY', right_on='C_CUSTKEY')

# Calculate revenue
lineitem['REVENUE'] = lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])

# Group by nation name and sum REVENUE
result = lineitem.groupby('N_NAME')['REVENUE'].sum().reset_index()

# Order the result by REVENUE
result = result.sort_values('REVENUE', ascending=False)

# Write to CSV
result.to_csv('query_output.csv', index=False)
