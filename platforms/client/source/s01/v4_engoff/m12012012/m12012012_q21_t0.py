import pandas as pd
from pymongo import MongoClient
import redis
import direct_redis

# MongoDB connection
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Retrieve data from MongoDB
nation_df = pd.DataFrame(list(mongo_db.nation.find({'N_NAME': 'SAUDI ARABIA'})))
supplier_df = pd.DataFrame(list(mongo_db.supplier.find()))
orders_df = pd.DataFrame(list(mongo_db.orders.find({'O_ORDERSTATUS': 'F'})))

# Redis connection
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from Redis
lineitem_df = pd.DataFrame(redis_client.get('lineitem'))

# Convert string data to list of dicts (assuming JSON strings are stored in Redis)
import json
lineitem_df = pd.DataFrame(json.loads(lineitem_df.iloc[0]))

# Process the data
# Find multi-supplier orders
multi_supplier_orders = lineitem_df.groupby('L_ORDERKEY').filter(lambda x: len(x) > 1)

# Find orders where there is only one supplier who failed to meet the commit date
failed_suppliers = multi_supplier_orders[multi_supplier_orders['L_COMMITDATE'] < multi_supplier_orders['L_RECEIPTDATE']]['L_SUPPKEY'].unique()

# Find suppliers from SAUDI ARABIA who kept the orders waiting
sa_nations_suppliers = supplier_df[supplier_df['S_NATIONKEY'].isin(nation_df['N_NATIONKEY'])]
suppliers_who_kept_orders_waiting = sa_nations_suppliers[sa_nations_suppliers['S_SUPPKEY'].isin(failed_suppliers)]

# Write the output to CSV file
suppliers_who_kept_orders_waiting.to_csv('query_output.csv', index=False)
