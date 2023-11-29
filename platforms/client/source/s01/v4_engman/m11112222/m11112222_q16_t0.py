import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb://mongodb:27017/')
tpch_mongo = mongo_client['tpch']
part_collection = tpch_mongo['part']
supplier_collection = tpch_mongo['supplier']

# Query parts in MongoDB
part_query = {
    "P_SIZE": {"$in": [49, 14, 23, 45, 19, 3, 36, 9]},
    "P_TYPE": {"$ne": "MEDIUM POLISHED"},
    "P_BRAND": {"$ne": "Brand#45"}
}
parts = pd.DataFrame(part_collection.find(part_query))

# Query suppliers in MongoDB
supplier_query = {
    "S_COMMENT": {"$not": {"$regex": "Customer.*Complaints"}}
}
suppliers = pd.DataFrame(supplier_collection.find(supplier_query))

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)
partsupp_str = redis_client.get('partsupp')
partsupp_df = pd.read_json(partsupp_str)

# Merge the dataframes
merge1 = pd.merge(parts, partsupp_df, left_on='P_PARTKEY', right_on='PS_PARTKEY')
result = pd.merge(merge1, suppliers, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Group and aggregate the results
final_result = result.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']) \
    .agg({'S_SUPPKEY': 'nunique'}) \
    .rename(columns={'S_SUPPKEY': 'SUPPLIER_COUNT'}) \
    .reset_index() \
    .sort_values(by=['SUPPLIER_COUNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], 
                 ascending=[False, True, True, True])

# Write to csv
final_result.to_csv('query_output.csv', index=False)
