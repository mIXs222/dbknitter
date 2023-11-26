# filename: query_mongodb_redis.py
from pymongo import MongoClient
import direct_redis
import pandas as pd

# MongoDB connection
mongo_client = MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
part_collection = mongo_db["part"]
partsupp_collection = mongo_db["partsupp"]

# Redis connection using direct_redis
redis_client = direct_redis.DirectRedis(host="redis", port=6379, db=0)

# Fetch data from MongoDB
parts_query = {
    'P_BRAND': {'$ne': 'Brand#45'},
    'P_TYPE': {'$not': {'$regex': '^MEDIUM POLISHED'}},
    'P_SIZE': {'$in': [49, 14, 23, 45, 19, 3, 36, 9]}
}
parts_df = pd.DataFrame(list(part_collection.find(parts_query, projection={'_id': False})))

# Fetch data from partsupp MongoDB
partsupp_df = pd.DataFrame(list(partsupp_collection.find(projection={'_id': False})))

# Fetch Supplier data from Redis and convert to DataFrame
supplier_data = redis_client.get('supplier')
supplier_df = pd.read_json(supplier_data)
supplier_df = supplier_df[supplier_df['S_COMMENT'].str.contains('Customer.*Complaints')]

# Merge parts and partsupp on part key
merged_df = pd.merge(partsupp_df, parts_df, left_on='PS_PARTKEY', right_on='P_PARTKEY')

# Remove suppliers with Customer Complaints
merged_df = merged_df[~merged_df['PS_SUPPKEY'].isin(supplier_df['S_SUPPKEY'])]

# Group by specified fields and count unique suppliers
result_df = merged_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']).agg(SUPPLIER_CNT=('PS_SUPPKEY', 'nunique'))

# Sort the results
result_df = result_df.reset_index().sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Write to CSV file
result_df.to_csv('query_output.csv', index=False)
