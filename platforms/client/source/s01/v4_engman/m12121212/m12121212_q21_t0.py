# query.py
from pymongo import MongoClient
import pandas as pd
import direct_redis
import csv

# MongoDB connection and data fetching
mongo_client = MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Fetch nation data from MongoDB
nation_data = mongodb['nation'].find({'N_NAME': 'SAUDI ARABIA'}, {'N_NATIONKEY': 1})
nation_keys = [doc['N_NATIONKEY'] for doc in nation_data]

# Fetch orders with status 'F' from MongoDB
orders_data = mongodb['orders'].find({'O_ORDERSTATUS': 'F'}, {'O_ORDERKEY': 1})
order_keys = [doc['O_ORDERKEY'] for doc in orders_data]

# Redis connection and data fetching
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Load supplier and lineitem tables from Redis
supplier_df = pd.DataFrame(eval(r.get('supplier')))
lineitem_df = pd.DataFrame(eval(r.get('lineitem')))

# Filter suppliers from SAUDI ARABIA
suppliers_in_nation = supplier_df[supplier_df['S_NATIONKEY'].isin(nation_keys)]

# Filter LineItems with orders having status 'F'
lineitems_with_orders = lineitem_df[lineitem_df['L_ORDERKEY'].isin(order_keys)]

# Group by order key to find multi-supplier orders
multi_supplier_orders = lineitems_with_orders.groupby('L_ORDERKEY').filter(lambda x: x['L_SUPPKEY'].nunique() > 1)

# Find suppliers who were the only one to fail to meet commitment
failed_suppliers = multi_supplier_orders.groupby(['L_ORDERKEY', 'L_SUPPKEY']).filter(lambda x: (x['L_RETURNFLAG'] == 'F').all())

# Combine the information to get the final result
result = failed_suppliers.merge(suppliers_in_nation, left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Count the number of waits per supplier
result = result.groupby('S_NAME').size().reset_index(name='NUMWAIT')
result = result.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Write the result to the csv file
result.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

# Close connections
mongo_client.close()
