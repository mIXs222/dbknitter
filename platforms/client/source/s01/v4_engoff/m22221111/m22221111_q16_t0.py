# python_code.py

import pymongo
import pandas as pd
import direct_redis
import csv

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
mongo_collection = mongo_db["partsupp"]

# Redis connection
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

def get_redis_data(table_name):
    data = redis_client.get(table_name)
    return pd.read_json(data) if data else pd.DataFrame()

# Getting partsupp data from MongoDB
partsupp_df = pd.DataFrame(list(mongo_collection.find()))

# Getting part and supplier data from Redis
part_df = get_redis_data('part')
supplier_df = get_redis_data('supplier')

# Filter parts that do not match criteria
filtered_parts = part_df[
    (part_df['P_BRAND'] != 'Brand#45') &
    (part_df['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9])) &
    (~part_df['P_TYPE'].str.contains("MEDIUM POLISHED"))
]

# Filter suppliers that do not have complaints
filtered_suppliers = supplier_df[
    ~supplier_df['S_COMMENT'].str.contains("Customer Complaints")
]

# Merge tables on supplier key
result = pd.merge(
    filtered_parts, 
    partsupp_df, 
    left_on='P_PARTKEY', 
    right_on='PS_PARTKEY'
)
result = pd.merge(
    result,
    filtered_suppliers,
    left_on='PS_SUPPKEY',
    right_on='S_SUPPKEY'
)

# Group and count
grouped_result = result.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']).agg({'S_SUPPKEY': 'count'}).reset_index()

# Sort the results
sorted_result = grouped_result.sort_values(by=['S_SUPPKEY', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Write to CSV file
sorted_result.to_csv('query_output.csv', index=False)
