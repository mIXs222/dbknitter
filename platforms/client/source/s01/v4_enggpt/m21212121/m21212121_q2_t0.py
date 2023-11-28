import pymongo
import pandas as pd
from direct_redis import DirectRedis
import csv

# Connect to MongoDB
client_mongo = pymongo.MongoClient("mongodb://mongodb:27017/")
db_mongo = client_mongo["tpch"]
region_mongo = pd.DataFrame(list(db_mongo["region"].find()))
supplier_mongo = pd.DataFrame(list(db_mongo["supplier"].find()))

# Filter suppliers from the 'EUROPE' region
europe_region_keys = region_mongo[region_mongo['R_NAME'] == 'EUROPE']['R_REGIONKEY'].tolist()
supplier_europe = supplier_mongo[supplier_mongo['S_NATIONKEY'].isin(europe_region_keys)]

# Connect to Redis
client_redis = DirectRedis(host='redis', port=6379, db=0)
nation_redis = pd.read_json(client_redis.get('nation'))
part_redis = pd.read_json(client_redis.get('part'))
partsupp_redis = pd.read_json(client_redis.get('partsupp'))

# Filter parts with a size of 15 and type containing 'BRASS'
part_filtered = part_redis[(part_redis['P_SIZE'] == 15) & (part_redis['P_TYPE'].str.contains('BRASS'))]

# Merge nation and supplier on nation key
supplier_nation = pd.merge(supplier_europe, nation_redis, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Filter nations within the 'EUROPE' region
supplier_nation_europe = supplier_nation[supplier_nation['N_REGIONKEY'].isin(europe_region_keys)]

# Merge parts and partsupp on part key
part_partsupp = pd.merge(part_filtered, partsupp_redis, left_on='P_PARTKEY', right_on='PS_PARTKEY')

# Find minimum supply cost for the part
min_supply_cost = part_partsupp.groupby('P_PARTKEY')['PS_SUPPLYCOST'].min().reset_index()

# Merge part with minimum supply cost
part_min_cost = pd.merge(part_filtered, min_supply_cost, left_on='P_PARTKEY', right_on='P_PARTKEY')

# Merge supplier with parts and partsupp
final_df = pd.merge(supplier_nation_europe, part_partsupp, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')

# Final filtering
final_filtered_df = final_df[
    final_df['PS_SUPPLYCOST'] == final_df['PS_SUPPLYCOST']
]

# Selecting necessary columns and sorting the dataframe
final_filtered_df = final_filtered_df[[
    'S_ACCTBAL', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT',
    'P_PARTKEY', 'P_MFGR', 'P_SIZE', 'N_NAME'
]].sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

# Write the final result to CSV
final_filtered_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_ALL)
