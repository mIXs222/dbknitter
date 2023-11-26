# query.py
import pymongo
import redis
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime
from functools import reduce

# Connect to MongoDB
client = pymongo.MongoClient("mongodb", 27017)
db = client['tpch']

# Load MongoDB tables
region_df = pd.DataFrame(list(db['region'].find({}, {'_id': False})))
supplier_df = pd.DataFrame(list(db['supplier'].find({}, {'_id': False})))
customer_df = pd.DataFrame(list(db['customer'].find({}, {'_id': False})))
lineitem_df = pd.DataFrame(list(db['lineitem'].find({}, {'_id': False})))

# Filter data according to SQL conditions
region_df = region_df[region_df.R_NAME == 'ASIA']
supplier_df = supplier_df[supplier_df['S_NATIONKEY'].isin(region_df['R_REGIONKEY'])]
customer_and_supplier_df = pd.merge(customer_df, supplier_df, left_on='C_NATIONKEY', right_on='S_NATIONKEY')
lineitem_filtered_df = lineitem_df[lineitem_df['L_SUPPKEY'].isin(supplier_df['S_SUPPKEY'])]

# Connect to Redis
r = DirectRedis(host='redis', port=6379, db=0)

# Load Redis tables using DirectRedis
nation_df = pd.read_json(r.get('nation'))
orders_df = pd.read_json(r.get('orders'))

# Convert dates to datetime for comparison
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])

# Filter orders by date
orders_df = orders_df[(orders_df['O_ORDERDATE'] >= datetime(1990, 1, 1)) & 
                      (orders_df['O_ORDERDATE'] < datetime(1995, 1, 1))]

# Merge tables based on the conditions
orders_and_customers_df = pd.merge(orders_df, customer_and_supplier_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
final_df = pd.merge(orders_and_customers_df, lineitem_filtered_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
final_df = pd.merge(final_df, nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Group by nation and calculate revenue
result_df = final_df.groupby('N_NAME').apply(
    lambda x: (x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT'])).sum()).reset_index(name='REVENUE')

# Sort the result
result_df = result_df.sort_values(by='REVENUE', ascending=False)

# Save to CSV
result_df.to_csv('query_output.csv', index=False)
