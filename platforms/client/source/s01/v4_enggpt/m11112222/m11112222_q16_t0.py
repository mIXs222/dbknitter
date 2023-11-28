import pymongo
import pandas as pd
import re
from direct_redis import DirectRedis

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
parts_collection = mongo_db["part"]
supplier_collection = mongo_db["supplier"]

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get parts data from MongoDB
parts_query = {
    "$and": [
        {"P_BRAND": {"$ne": "Brand#45"}},
        {"P_TYPE": {"$not": re.compile(r'^MEDIUM POLISHED')}},
        {"P_SIZE": {"$in": [49, 14, 23, 45, 19, 3, 36, 9]}},
    ]
}
parts_projection = {"_id": False}
parts_df = pd.DataFrame(list(parts_collection.find(parts_query, parts_projection)))

# Get partsupp data from Redis
partsupp_df = pd.read_msgpack(redis_client.get('partsupp'))

# Get supplier data from MongoDB and filter by comments
supplier_query = {"S_COMMENT": {"$not": re.compile(r'Customer Complaints')}}
supplier_projection = {"_id": False}
supplier_df = pd.DataFrame(list(supplier_collection.find(supplier_query, supplier_projection)))

# Merge dataframes
merged_df = pd.merge(partsupp_df, parts_df, left_on='PS_PARTKEY', right_on='P_PARTKEY')
merged_df = pd.merge(merged_df, supplier_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Perform aggregation and grouping
results = (
    merged_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])
    .agg(SUPPLIER_CNT=pd.NamedAgg(column="S_SUPPKEY", func="nunique"))
    .reset_index()
)

# Sort the results
sorted_results = results.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'],
                                     ascending=[False, True, True, True])

# Write the results to CSV
sorted_results.to_csv('query_output.csv', index=False)
