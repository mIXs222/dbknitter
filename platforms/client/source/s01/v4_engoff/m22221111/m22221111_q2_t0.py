from pymongo import MongoClient
import direct_redis
import pandas as pd

# MongoDB connection and query
client = MongoClient('mongodb', 27017)
mongodb = client.tpch
partsupp = list(mongodb.partsupp.find())

# Convert partsupp data to DataFrame
df_partsupp = pd.DataFrame(partsupp)

# Redis connection
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get tables data from Redis and convert to DataFrame
df_nation = pd.DataFrame(redis_client.get('nation'))
df_region = pd.DataFrame(redis_client.get('region'))
df_part = pd.DataFrame(redis_client.get('part'))
df_supplier = pd.DataFrame(redis_client.get('supplier'))

# Filter part by type BRASS and size 15
df_part_filtered = df_part[(df_part["P_TYPE"] == "BRASS") & (df_part["P_SIZE"] == 15)]

# Filter regions by EUROPE
df_region_europe = df_region[df_region["R_NAME"] == "EUROPE"]

# Join tables to analyze the data
df = (
    df_part_filtered
    .merge(df_partsupp, how='inner', left_on='P_PARTKEY', right_on='PS_PARTKEY')
    .merge(df_supplier, how='inner', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
    .merge(df_nation, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    .merge(df_region_europe, how='inner', left_on='N_REGIONKEY', right_on='R_REGIONKEY')
)

# Find minimum PS_SUPPLYCOST for each part
df_min_cost = df.groupby('P_PARTKEY')['PS_SUPPLYCOST'].min().reset_index()

# Filter suppliers who offer min cost
df = df.merge(df_min_cost, how='inner', on=['P_PARTKEY', 'PS_SUPPLYCOST'])

# Sorting the results according to the specified rules
df = df.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

# Selecting the required columns
output_columns = [
    'S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR', 'S_ADDRESS',
    'S_PHONE', 'S_COMMENT', 'PS_SUPPLYCOST'
]
df_output = df[output_columns]

# Write to query_output.csv
df_output.to_csv('query_output.csv', index=False)
