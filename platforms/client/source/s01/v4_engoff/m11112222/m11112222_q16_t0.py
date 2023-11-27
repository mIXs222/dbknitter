import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
part_col = mongo_db["part"]
supplier_col = mongo_db["supplier"]

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Query MongoDB for parts
part_query = {
    'P_SIZE': {'$in': [49, 14, 23, 45, 19, 3, 36, 9]},
    'P_BRAND': {'$ne': 'Brand#45'},
    'P_TYPE': {'$not': {'$regex': 'MEDIUM POLISHED'}}
}
parts = pd.DataFrame(list(part_col.find(part_query)))

# Query MongoDB for suppliers
supplier_query = {
    'S_COMMENT': {'$not': {'$regex': 'Customer.*Complaints'}}
}
suppliers = pd.DataFrame(list(supplier_col.find(supplier_query)))

# Query Redis for partsupp
partsupp_data = redis_client.get("partsupp")
partsupp = pd.read_msgpack(partsupp_data)

# Perform the join and group operation
result = parts.merge(partsupp, how='inner', left_on='P_PARTKEY', right_on='PS_PARTKEY')
result = result.merge(suppliers, how='inner', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Group by criteria and count the number of suppliers
group_attributes = ['P_BRAND', 'P_TYPE', 'P_SIZE']
suppliers_count = result.groupby(group_attributes)['S_SUPPKEY'].nunique().reset_index()
suppliers_count.columns = group_attributes + ['SUPPLIER_COUNT']

# Sort the results
sorted_suppliers_count = suppliers_count.sort_values(by=['SUPPLIER_COUNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Output to CSV file
sorted_suppliers_count.to_csv("query_output.csv", index=False)
