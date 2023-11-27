# File: query_execute.py

import pymongo
import redis
import pandas as pd

# Connecting to MongoDB
def connect_to_mongodb():
    client = pymongo.MongoClient("mongodb", 27017)
    db = client["tpch"]
    return db

# Fetching data from MongoDB
def fetch_mongodb_data(db):
    part_collection = db["part"]
    parts_data = list(part_collection.find({"$and": [
        {"P_BRAND": {"$ne": "Brand#45"}},
        {"P_TYPE": {"$not": {"$regex": "MEDIUM POLISHED"}}},
        {"P_SIZE": {"$in": [49, 14, 23, 45, 19, 3, 36, 9]}}
    ]}, projection={'_id': False}))
    return pd.DataFrame(parts_data)

# Connecting to Redis
def connect_to_redis():
    rd = redis.StrictRedis(host="redis", port=6379, db=0, decode_responses=True)
    return rd

# Fetching data from Redis
def fetch_redis_data(rd):
    supplier_data = rd.get('supplier')
    partsupp_data = rd.get('partsupp')

    supplier_df = pd.read_json(supplier_data)
    partsupp_df = pd.read_json(partsupp_data)

    # Filter suppliers without complaints (assuming complaints are indicated in S_COMMENT)
    supplier_no_complaints = supplier_df[~supplier_df['S_COMMENT'].str.contains("Customer Complaints")]

    return partsupp_df.merge(supplier_no_complaints, left_on='PS_SUPPKEY', right_on='S_SUPPKEY', how='inner')

# Main logic
def main():
    # Database connections
    mongodb_db = connect_to_mongodb()
    redis_db = connect_to_redis()

    # Fetch data
    parts_df = fetch_mongodb_data(mongodb_db)
    partsupp_df = fetch_redis_data(redis_db)

    # Combine DataFrames on partkey
    combined_df = partsupp_df.merge(parts_df, left_on='PS_PARTKEY', right_on='P_PARTKEY')

    # Count suppliers and sort the results as specified
    result = combined_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']).size().reset_index(name='SUPPLIER_COUNT')
    result = result.sort_values(by=['SUPPLIER_COUNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

    # Write results to CSV
    result.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
