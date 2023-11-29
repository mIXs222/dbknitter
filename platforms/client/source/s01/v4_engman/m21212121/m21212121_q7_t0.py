from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import direct_redis

# MongoDB connection
client = MongoClient('mongodb', 27017)
mongo_db = client['tpch']
supplier_col = mongo_db['supplier']
customer_col = mongo_db['customer']
lineitem_col = mongo_db['lineitem']

# Redis connection
redis = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get data from MongoDB
suppliers_df = pd.DataFrame(list(supplier_col.find({}, {'_id': 0})))
customers_df = pd.DataFrame(list(customer_col.find({}, {'_id': 0})))
lineitems_df = pd.DataFrame(list(lineitem_col.find({}, {'_id': 0})))

# Get data from Redis
nation_df = pd.read_json(redis.get('nation'), orient='records')
orders_df = pd.read_json(redis.get('orders'), orient='records')

# Filter nation
nation_df = nation_df[nation_df["N_NAME"].isin(["INDIA", "JAPAN"])]

# Merge dataframes
supp_nation = suppliers_df.merge(nation_df, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY')
cust_orders = customers_df.merge(orders_df, how='inner', left_on='C_CUSTKEY', right_on='O_CUSTKEY')
df = lineitems_df.merge(cust_orders, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
df = df.merge(supp_nation, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Filter years and cross-nation conditions
df['L_SHIPDATE'] = pd.to_datetime(df['L_SHIPDATE'])
df = df[(df['L_SHIPDATE'].dt.year.isin([1995, 1996])) & 
         (df['N_NAME_x'] != df['N_NAME_y']) & 
         (df['N_NAME_x'].isin(["INDIA", "JAPAN"])) & 
         (df['N_NAME_y'].isin(["INDIA", "JAPAN"]))]

# Compute revenues
df['REVENUE'] = df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT'])
df['L_YEAR'] = df['L_SHIPDATE'].dt.year

# Group and sort
result_df = df.groupby(['N_NAME_x', 'L_YEAR', 'N_NAME_y'])['REVENUE'] \
              .sum().reset_index().sort_values(['N_NAME_y', 'N_NAME_x', 'L_YEAR'])

# Rename columns
result_df.rename(columns={'N_NAME_x': 'CUST_NATION', 'L_YEAR': 'L_YEAR', 'REVENUE': 'REVENUE', 'N_NAME_y': 'SUPP_NATION'}, inplace=True)

# Output columns order
result_df = result_df[['CUST_NATION', 'L_YEAR', 'REVENUE', 'SUPP_NATION']]

# Write to CSV
result_df.to_csv('query_output.csv', index=False)

