from pymongo import MongoClient
import direct_redis
import pandas as pd
from datetime import datetime

# Connect to MongoDB
mongo_client = MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']

# Load MongoDB tables to pandas DataFrames
lineitem_df = pd.DataFrame(list(mongo_db.lineitem.find()))
partsupp_df = pd.DataFrame(list(mongo_db.partsupp.find()))

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Load Redis tables to pandas DataFrames
nation_df = pd.read_json(redis_client.get('nation'))
supplier_df = pd.read_json(redis_client.get('supplier'))

# Data processing to execute the query

# Join tables
joined_df = lineitem_df.merge(partsupp_df, how='inner', left_on=['L_PARTKEY','L_SUPPKEY'], right_on=['PS_PARTKEY','PS_SUPPKEY'])
joined_df = joined_df.merge(supplier_df, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')
joined_df = joined_df.merge(nation_df, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Filter by the specified 'dim' in the part names (L_COMMENT) - replace 'dim' with the actual dimension you are searching for
# joined_df = joined_df[joined_df['L_COMMENT'].str.contains('dim')]

# Calculate profit
joined_df['PROFIT'] = (joined_df['L_EXTENDEDPRICE'] * (1 - joined_df['L_DISCOUNT'])) - (joined_df['PS_SUPPLYCOST'] * joined_df['L_QUANTITY'])

# Extract year from dates
joined_df['YEAR'] = pd.to_datetime(joined_df['L_SHIPDATE']).dt.year

# Group by nation and year, and calculate total profit
result_df = (joined_df.groupby(['N_NAME', 'YEAR'])
             .agg({'PROFIT': 'sum'})
             .sort_values(by=['N_NAME', 'YEAR'], ascending=[True, False])
             .reset_index())

# Save to CSV
result_df.to_csv('query_output.csv', index=False)
