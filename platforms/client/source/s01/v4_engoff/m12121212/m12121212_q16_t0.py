# python code (query.py)
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MongoDB connection setup
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
part_collection = mongo_db["part"]
partsupp_collection = mongo_db["partsupp"]

# Query parts with the specified conditions from MongoDB
part_conditions = {
    "P_SIZE": {"$in": [49, 14, 23, 45, 19, 3, 36, 9]},
    "P_TYPE": {"$ne": "MEDIUM POLISHED"},
    "P_BRAND": {"$ne": "Brand#45"}
}
part_fields = {
    "P_PARTKEY": 1,
    "_id": 0
}
part_docs = list(part_collection.find(part_conditions, part_fields))

# Convert MongoDB query results to DataFrame
part_df = pd.DataFrame(part_docs)

# Query partsupp to find matching suppliers for the parts from MongoDB
partsupp_fields = {
    "PS_PARTKEY": 1,
    "PS_SUPPKEY": 1,
    "_id": 0
}
partsupp_docs = list(partsupp_collection.find({"PS_PARTKEY": {"$in": part_df["P_PARTKEY"].tolist()}}, partsupp_fields))

# Convert MongoDB query results to DataFrame
partsupp_df = pd.DataFrame(partsupp_docs)

# Redis connection setup
redis_client = DirectRedis(host="redis", port=6379, db=0)

# Retrieve supplier table from Redis
supplier_data = redis_client.get("supplier")
supplier_df = pd.read_json(supplier_data)

# Use the DataFrame query method to further filter suppliers
supplier_df = supplier_df.query("S_COMMENT.str.contains('Customer Complaints') == False", engine='python')

# Merge the DataFrames to find out how many suppliers can supply the parts
result_df = partsupp_df.merge(supplier_df, how='inner', left_on="PS_SUPPKEY", right_on="S_SUPPKEY")

# Group by S_SUPPKEY (supplier key) and count
final_df = result_df.groupby(['S_SUPPKEY']).size().reset_index(name='count')

# Sort the results as specified in the requirements
sorted_final_df = final_df.sort_values(by=['count', 'S_SUPPKEY'], ascending=[False, True])

# Write output to CSV file
sorted_final_df.to_csv('query_output.csv', index=False)
