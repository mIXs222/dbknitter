# query.py

import pymongo
from bson import Regex
import direct_redis
import pandas as pd

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = client["tpch"]
supplier = pd.DataFrame(list(mongo_db.supplier.find()))
partsupp = pd.DataFrame(list(mongo_db.partsupp.find()))

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
nation_df = pd.read_json(redis_client.get('nation'))
region_df = pd.read_json(redis_client.get('region'))
part_df = pd.read_json(redis_client.get('part'))

# Filter the data based on the SQL conditions
filtered_parts = part_df[(part_df['P_SIZE'] == 15) & (part_df['P_TYPE'].str.contains('BRASS'))]
filtered_parts = filtered_parts.rename(columns={'P_PARTKEY': 'PS_PARTKEY'})

# Joining the DataFrames to mimic SQL joins
merged_df = supplier.merge(partsupp, on='S_SUPPKEY') \
                    .merge(filtered_parts, on='PS_PARTKEY') \
                    .merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY') \
                    .merge(region_df, left_on='N_REGIONKEY', right_on='R_REGIONKEY')

# Filtering on region name 'EUROPE'
eu_df = merged_df[merged_df['R_NAME'] == 'EUROPE']

# Calculating the minimum PS_SUPPLYCOST per part for the EUROPE region
min_cost_df = eu_df.groupby(['PS_PARTKEY']).agg({'PS_SUPPLYCOST': 'min'}).reset_index()
min_cost_df = min_cost_df.rename(columns={'PS_SUPPLYCOST': 'MIN_PS_SUPPLYCOST'})

# Join with the original dataframe to filter on the minimum cost.
result_df = eu_df.merge(min_cost_df, how='inner', left_on=['PS_PARTKEY', 'PS_SUPPLYCOST'], right_on=['PS_PARTKEY', 'MIN_PS_SUPPLYCOST'])

# Select the required columns
final_df = result_df[['S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT']]

# Sorting as per the SQL ORDER BY clause
final_df = final_df.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

# Write to CSV file
final_df.to_csv("query_output.csv", index=False)
