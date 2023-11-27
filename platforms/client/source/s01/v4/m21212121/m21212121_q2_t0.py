import pymongo
import pandas as pd
from bson import json_util
import json
from direct_redis import DirectRedis

# MongoDB connection details
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
collection_region = mongo_db["region"]
collection_supplier = mongo_db["supplier"]

# Redis connection details
redis_client = DirectRedis(host='redis', port=6379, db=0)


def redis_to_df(table_name):
    table_data = redis_client.get(table_name)
    table_data = json.loads(table_data)
    dataframe = pd.DataFrame(table_data)
    return dataframe


# Connect to Redis and pull in relevant data frames
nation_df = redis_to_df('nation')
part_df = redis_to_df('part')
partsupp_df = redis_to_df('partsupp')

# MongoDB queries & DataFrames
region_df = pd.DataFrame(list(collection_region.find({"R_NAME": "EUROPE"})))
supplier_df = pd.DataFrame(list(collection_supplier.find()))

# Merge Redis dataframes
partsupp_part = pd.merge(partsupp_df, part_df, left_on="PS_PARTKEY", right_on="P_PARTKEY")
partsupp_part_nation = pd.merge(partsupp_part, nation_df, left_on="PS_SUPPKEY", right_on="N_NATIONKEY")
merged_df = pd.merge(partsupp_part_nation, supplier_df, left_on="PS_SUPPKEY", right_on="S_SUPPKEY")

# Filter according to queries
filtered_df = merged_df[(merged_df['P_SIZE'] == 15) & 
                        (merged_df['P_TYPE'].str.contains('BRASS')) &
                        (merged_df['N_REGIONKEY'] == region_df['R_REGIONKEY'].iloc[0])]

# Calculate minimum PS_SUPPLYCOST
min_supply_cost = filtered_df['PS_SUPPLYCOST'].min()
filtered_df = filtered_df[filtered_df['PS_SUPPLYCOST'] == min_supply_cost]

# Select required columns
result_df = filtered_df[['S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR', 
                         'S_ADDRESS', 'S_PHONE', 'S_COMMENT']]

# Sort by the specified columns
result_df = result_df.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

# Output to CSV
result_df.to_csv('query_output.csv', index=False)
