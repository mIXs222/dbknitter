# query.py
import pymongo
import direct_redis
import pandas as pd

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
# Retrieve data from MongoDB collections
nation_df = pd.DataFrame(list(mongo_db.nation.find()))
part_df = pd.DataFrame(list(mongo_db.part.find({'P_SIZE': 15, 'P_TYPE': {'$regex': 'BRASS'}})))
partsupp_df = pd.DataFrame(list(mongo_db.partsupp.find()))

# Redis connection
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
# Retrieve data from Redis
region_df = pd.read_msgpack(redis_client.get('region'))
supplier_df = pd.read_msgpack(redis_client.get('supplier'))

# Filtering for 'EUROPE' region
europe_region_df = region_df[region_df.R_NAME == 'EUROPE']
# Joining and filtering the dataframe
europe_nation_df = nation_df[nation_df.N_REGIONKEY.isin(europe_region_df.R_REGIONKEY)]

# Filtering suppliers in 'EUROPE'
europe_supplier_df = supplier_df[supplier_df.S_NATIONKEY.isin(europe_nation_df.N_NATIONKEY)]

# Filtering parts based on PS_PARTKEY from partsupp_df and P_PARTKEY from part_df
part_in_partsupp_df = partsupp_df[partsupp_df.PS_PARTKEY.isin(part_df.P_PARTKEY)]
# Finding minimum PS_SUPPLYCOST for each part
min_cost_partsupp_df = part_in_partsupp_df.groupby('PS_PARTKEY')['PS_SUPPLYCOST'].min().reset_index()
# Joining to get part details having the minimum supply cost
min_cost_part_df = pd.merge(min_cost_partsupp_df, part_df, left_on="PS_PARTKEY", right_on="P_PARTKEY")

# Joining to get parts supplied by suppliers in the 'EUROPE' region
europe_supplier_partsupp_df = europe_supplier_df.merge(partsupp_df, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')
# Filtering parts with minimum supply cost from 'EUROPE' region
final_parts_df = europe_supplier_partsupp_df.merge(min_cost_part_df, on=['PS_PARTKEY', 'PS_SUPPLYCOST'])

# Selecting final columns
final_columns = ['S_ACCTBAL', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT', 'P_PARTKEY', 'P_MFGR', 'P_SIZE']
final_df = final_parts_df[final_columns]

# Sorting the results as per the query
sorted_final_df = final_df.sort_values(by=['S_ACCTBAL', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True])

# Saving results to CSV
sorted_final_df.to_csv('query_output.csv', index=False)
