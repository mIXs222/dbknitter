# query.py
import pymongo
import redis
from direct_redis import DirectRedis
import pandas as pd
from datetime import datetime

# MongoDB connection
client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = client["tpch"]
nation_collection = mongodb["nation"]

# Redis connection
r = DirectRedis(host="redis", port=6379, db=0)

# Fetch data from MongoDB
nations = pd.DataFrame(list(nation_collection.find({}, {'_id': 0})))

# Fetch data from Redis, converting to Pandas DataFrames
customers_df = pd.read_json(r.get("customer"))
orders_df = pd.read_json(r.get("orders"))
lineitem_df = pd.read_json(r.get("lineitem"))

# Filtering the data
filtered_orders = orders_df[
    (orders_df['O_ORDERDATE'] >= '1993-10-01') &
    (orders_df['O_ORDERDATE'] <= '1993-12-31')
]
filtered_lineitem = lineitem_df[
    lineitem_df['L_RETURNFLAG'] == 'R'
]

# Merging the datasets
merged_df = pd.merge(customers_df, filtered_orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = pd.merge(merged_df, filtered_lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
merged_df = pd.merge(merged_df, nations, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Calculating revenue
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Aggregating by customer
aggregated_data = merged_df.groupby([
    'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT'
]).agg({
    'REVENUE': 'sum'
}).reset_index()

# Sorting the results
sorted_data = aggregated_data.sort_values(
    by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'],
    ascending=[True, True, True, False]
)

# Writing the result to a CSV file
sorted_data.to_csv('query_output.csv', index=False)
