from pymongo import MongoClient
import pandas as pd
import redis
import direct_redis
import csv
from datetime import datetime

# Connect to the MongoDB server
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Load MongoDB tables into Pandas DataFrames
nation_df = pd.DataFrame(list(mongo_db.nation.find({}, {'_id': 0})))
part_df = pd.DataFrame(list(mongo_db.part.find({}, {'_id': 0})))
partsupp_df = pd.DataFrame(list(mongo_db.partsupp.find({}, {'_id': 0})))

# Connect to the Redis server
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Load Redis tables into Pandas DataFrames
supplier_df = pd.read_msgpack(redis_client.get('supplier'))
lineitem_df = pd.read_msgpack(redis_client.get('lineitem'))

# Merge the necessary DataFrames to perform the query
merged_df = lineitem_df.merge(
    supplier_df, how='left', left_on='L_SUPPKEY', right_on='S_SUPPKEY'
).merge(
    nation_df, how='left', left_on='S_NATIONKEY', right_on='N_NATIONKEY'
).merge(
    partsupp_df, how='left', left_on=['L_PARTKEY', 'L_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY']
)

# Filter parts that contain a specified dim in their names
# Assuming 'dim' variable contains the specified dim
dim = 'SPECIFIED_DIM'
filtered_df = merged_df[merged_df['P_NAME'].str.contains(dim)]

# Calculate the profit
filtered_df['YEAR'] = filtered_df['L_SHIPDATE'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d").year)
filtered_df['PROFIT'] = (filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])) - (filtered_df['PS_SUPPLYCOST'] * filtered_df['L_QUANTITY'])

# Group by nation and year, and sum the profit
output_df = filtered_df.groupby(['N_NAME', 'YEAR'])['PROFIT'].sum().reset_index()

# Sort the results as per the requirement
output_df.sort_values(by=['N_NAME', 'YEAR'], ascending=[True, False], inplace=True)

# Write the query output to a CSV file
output_df.to_csv('query_output.csv', index=False)
