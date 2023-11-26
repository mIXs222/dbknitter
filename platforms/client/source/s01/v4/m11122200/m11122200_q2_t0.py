import pymongo
import pandas as pd
import direct_redis

# Connecting to the MongoDB instance
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
mongo_nation = pd.DataFrame(list(mongo_db["nation"].find({})))
mongo_region = pd.DataFrame(list(mongo_db["region"].find({})))
mongo_part = pd.DataFrame(list(mongo_db["part"].find({"P_SIZE": 15, "P_TYPE": {"$regex": ".*BRASS.*"}})))

# Connecting to the Redis instance
redis_client = direct_redis.DirectRedis(host='redis', port=6379)
redis_supplier = pd.read_json(redis_client.get('supplier'))
redis_partsupp = pd.read_json(redis_client.get('partsupp'))

# Joining and filtering the Redis tables
redis_join = pd.merge(redis_supplier, redis_partsupp, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')
europe_regions = mongo_region[mongo_region['R_NAME'] == 'EUROPE']
europe_nations = pd.merge(europe_regions, mongo_nation, left_on='R_REGIONKEY', right_on='N_REGIONKEY')

redis_join = pd.merge(redis_join, europe_nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
redis_join = redis_join[redis_join['R_NAME'] == 'EUROPE']  

# Calculating minimum PS_SUPPLYCOST for parts that satisfy the conditions
min_ps_supplycost = redis_join.groupby('PS_PARTKEY')['PS_SUPPLYCOST'].min().reset_index()
min_ps_supplycost_filtered = min_ps_supplycost[min_ps_supplycost['PS_PARTKEY'].isin(mongo_part['P_PARTKEY'])]

# Final join
final_result = pd.merge(mongo_part, redis_join, left_on='P_PARTKEY', right_on='PS_PARTKEY')
final_result = pd.merge(final_result, min_ps_supplycost_filtered, on=['PS_PARTKEY', 'PS_SUPPLYCOST'])

# Selecting required columns and sorting
final_result = final_result[['S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT']]
final_result.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True], inplace=True)

# Writing result to CSV
final_result.to_csv('query_output.csv', index=False)
