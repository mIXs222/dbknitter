# import necessary libraries
from pymongo import MongoClient
import pandas as pd
from direct_redis import DirectRedis
import sqlite3

# Establish MongoDB connection
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Load MongoDB tables into pandas dataframes
nation_df = pd.DataFrame(list(mongo_db.nation.find()))
supplier_df = pd.DataFrame(list(mongo_db.supplier.find()))
part_df = pd.DataFrame(list(mongo_db.part.find()))

# Establish Redis connection and load data
redis_client = DirectRedis(host='redis', port=6379, db=0)
partsupp_df = pd.read_json(redis_client.get('partsupp'))
orders_df = pd.read_json(redis_client.get('orders'))
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Start processing the query
# Filter part_df with P_NAME like '%dim%'
part_df = part_df[part_df['P_NAME'].str.contains('dim')]

# Merge the dataframes on specified keys for join operation
merged_df = (
    lineitem_df.merge(part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')
    .merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    .merge(partsupp_df, how='inner', left_on=['L_PARTKEY', 'L_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY'])
    .merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    .merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
)

# Project useful columns and calculate amount
merged_df['O_YEAR'] = pd.to_datetime(merged_df['O_ORDERDATE']).dt.year
merged_df['AMOUNT'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT']) - merged_df['PS_SUPPLYCOST'] * merged_df['L_QUANTITY']

# Group by, Perform aggregation and sort
result_df = (
    merged_df[['N_NAME', 'O_YEAR', 'AMOUNT']]
    .groupby(['N_NAME', 'O_YEAR'])
    .agg(SUM_PROFIT=pd.NamedAgg(column='AMOUNT', aggfunc='sum'))
    .reset_index()
    .sort_values(by=['N_NAME', 'O_YEAR'], ascending=[True, False])
)

# Write to CSV
result_df.to_csv('query_output.csv', index=False)
