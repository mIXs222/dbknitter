from pymongo import MongoClient
import pandas as pd
import direct_redis

# MongoDB Connection
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Redis Connection
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Read data from MongoDB
nation_df = pd.DataFrame(list(mongo_db.nation.find()))
region_df = pd.DataFrame(list(mongo_db.region.find()))
supplier_df = pd.DataFrame(list(mongo_db.supplier.find()))

# Read data from Redis using 'get' for each table and convert to DataFrame
customer_df = pd.read_json(redis_client.get('customer'))
orders_df = pd.read_json(redis_client.get('orders'))
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Convert string dates to datetime objects
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])

# Filtering by date range
date_start = pd.Timestamp('1990-01-01')
date_end = pd.Timestamp('1995-01-01')
orders_df = orders_df[(orders_df['O_ORDERDATE'] >= date_start) & (orders_df['O_ORDERDATE'] <= date_end)]

# Filter region by ASIA
asia_region = region_df[region_df['R_NAME'] == 'ASIA']
asia_nations = nation_df[nation_df['N_REGIONKEY'].isin(asia_region['R_REGIONKEY'])]

# Joining data
lineitem_orders = lineitem_df.merge(orders_df, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
supplier_nation = supplier_df.merge(asia_nations, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY')
customer_nation = customer_df.merge(asia_nations, how='inner', left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Join lineitem_orders with supplier_nation and customer_nation
final_df = lineitem_orders.merge(supplier_nation, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')
final_df = final_df.merge(customer_nation, how='inner', left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Calculate Revenue
final_df['REVENUE'] = final_df['L_EXTENDEDPRICE'] * (1 - final_df['L_DISCOUNT'])

# Group by Nation and calculate revenue
result_df = final_df.groupby('N_NAME')['REVENUE'].sum().reset_index()

# Sort by Revenue in descending order
result_df = result_df.sort_values(by='REVENUE', ascending=False)

# Select required columns
result_df = result_df[['N_NAME', 'REVENUE']]

# Write result to CSV
result_df.to_csv('query_output.csv', index=False)
