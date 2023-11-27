import pymongo
import pandas as pd
from bson import ObjectId
from direct_redis import DirectRedis

# MongoDB connection setup
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
supplier_collection = mongo_db["supplier"]

# Redis connection setup
redis = DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from MongoDB
suppliers_with_complaints = list(supplier_collection.find({'S_COMMENT': {'$regex': '.*Customer.*Complaints.*'}}, {'S_SUPPKEY': 1}))
suppliers_with_complaints_set = set([doc['S_SUPPKEY'] for doc in suppliers_with_complaints])

# Retrieve data from Redis
parts_df = pd.read_json(redis.get('part'))  # Redis should return data as a JSON string
partsupp_df = pd.read_json(redis.get('partsupp'))

# Filter parts according to the criteria
filtered_parts = parts_df[
    (parts_df['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9])) &
    (parts_df['P_TYPE'] != 'MEDIUM POLISHED') &
    (parts_df['P_BRAND'] != 'Brand#45')
]

# Merge tables and apply final filters
result = (
    filtered_parts
    .merge(partsupp_df, left_on='P_PARTKEY', right_on='PS_PARTKEY')
    .query('~PS_SUPPKEY in @suppliers_with_complaints_set')
    .groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])
    .agg({'PS_SUPPKEY': 'nunique'})
    .rename(columns={'PS_SUPPKEY': 'supplier_count'})
    .sort_values(['supplier_count', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])
    .reset_index()
)

# Write results to a CSV file
result.to_csv('query_output.csv', index=False)
