# query.py
from pymongo import MongoClient
import pandas as pd
import direct_redis
import os

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
mongodb = client['tpch']

# Retrieve data from MongoDB
nation_df = pd.DataFrame(list(mongodb.nation.find()))
orders_df = pd.DataFrame(list(mongodb.orders.find()))

# Connect to Redis and retrieve data
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
region_df = pd.read_json(r.get('region'))
supplier_df = pd.read_json(r.get('supplier'))
customer_df = pd.read_json(r.get('customer'))
lineitem_df = pd.read_json(r.get('lineitem'))

# Perform SQL-like join and filter operations using Pandas
merged_df = (
    customer_df
    .merge(orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
    .merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    .merge(supplier_df, left_on=['C_NATIONKEY', 'L_SUPPKEY'], right_on=['S_NATIONKEY', 'S_SUPPKEY'])
    .merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    .merge(region_df, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
)

# Filter for the required conditions
filtered_df = merged_df.loc[
    (merged_df['R_NAME'] == 'ASIA') &
    (merged_df['O_ORDERDATE'] >= '1990-01-01') &
    (merged_df['O_ORDERDATE'] < '1995-01-01')
]

# Calculate revenue
filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])

# Group by nation name and sum revenue
result_df = (
    filtered_df.groupby('N_NAME')['REVENUE']
    .sum()
    .reset_index()
    .sort_values(by='REVENUE', ascending=False)
)

# Save to CSV
result_df.to_csv('query_output.csv', index=False)
