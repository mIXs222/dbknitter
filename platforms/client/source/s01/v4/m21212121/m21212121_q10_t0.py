# query.py
import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]

# Read MongoDB collections into DataFrames
customers_df = pd.DataFrame(list(mongo_db["customer"].find()))
lineitems_df = pd.DataFrame(list(mongo_db["lineitem"].find()))

# Connect to Redis
redis_client = DirectRedis(host="redis", port=6379, db=0)

# Read Redis keys into DataFrames
orders_df = pd.read_msgpack(redis_client.get('orders'))
nations_df = pd.read_msgpack(redis_client.get('nation'))

# Convert strings to datetime
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])

# Filter data based on conditions
filtered_orders = orders_df[
    (orders_df['O_ORDERDATE'] >= datetime(1993, 10, 1)) &
    (orders_df['O_ORDERDATE'] < datetime(1994, 1, 1))
]

filtered_lineitems = lineitems_df[lineitems_df['L_RETURNFLAG'] == 'R']

# Merge/join dataframes
merged_df = customers_df.merge(filtered_orders, how='inner', left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = merged_df.merge(filtered_lineitems, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')
merged_df = merged_df.merge(nations_df, how='inner', left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Compute revenue
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Perform the grouping
grouped_df = merged_df.groupby(['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_PHONE', 'N_NAME', 'C_ADDRESS', 'C_COMMENT'], as_index=False).agg({'REVENUE': 'sum'})

# Sort the results
sorted_df = grouped_df.sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[False, True, True, False])

# Select the required columns
final_df = sorted_df[['C_CUSTKEY', 'C_NAME', 'REVENUE', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']]

# Save the output to a CSV file
final_df.to_csv('query_output.csv', index=False)
