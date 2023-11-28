# Filename: query_analysis.py
import pandas as pd
import pymongo
from direct_redis import DirectRedis
import json

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]

# Retrieve collections from MongoDb
customers = pd.DataFrame(list(mongo_db["customer"].find()))
lineitems = pd.DataFrame(list(mongo_db["lineitem"].find()))
orders = pd.DataFrame(list(mongo_db["orders"].find()))

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)
nation = pd.read_json(redis_client.get('nation'))
region = pd.read_json(redis_client.get('region'))
part = pd.read_json(redis_client.get('part'))

# Filtering and setting data types
part = part[part['P_TYPE'] == 'SMALL PLATED COPPER']
region = region[region['R_NAME'] == 'ASIA']
orders['O_ORDERDATE'] = pd.to_datetime(orders['O_ORDERDATE'])
orders = orders[(orders['O_ORDERDATE'].dt.year >= 1995) & (orders['O_ORDERDATE'].dt.year <= 1996)]

# Merge the dataframes to prepare for market share calculation
merged_df = (
    lineitems.merge(part, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')
    .merge(orders, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    .merge(customers, how='inner', left_on='O_CUSTKEY', right_on='C_CUSTKEY')
    .merge(nation, how='inner', left_on='C_NATIONKEY', right_on='N_NATIONKEY')
    .merge(region, how='inner', left_on='N_REGIONKEY', right_on='R_REGIONKEY')
)

# Calculating volume and market share
merged_df['volume'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
india_volume = merged_df[merged_df['N_NAME'] == 'INDIA'].groupby(merged_df['O_ORDERDATE'].dt.year)['volume'].sum()
total_volume = merged_df.groupby(merged_df['O_ORDERDATE'].dt.year)['volume'].sum()
market_share = (india_volume / total_volume).reset_index()
market_share.columns = ['year', 'market_share']

# Writing the results to a CSV file
market_share.sort_values('year').to_csv('query_output.csv', index=False)
