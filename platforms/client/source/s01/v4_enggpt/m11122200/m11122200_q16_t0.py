import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
mongo_part = pd.DataFrame(list(mongo_db.part.find(
    {
        "P_BRAND": {"$ne": "Brand#45"},
        "P_TYPE": {"$not": {"$regex": "^MEDIUM POLISHED"}},
        "P_SIZE": {"$in": [49, 14, 23, 45, 19, 3, 36, 9]}
    },
    {
        "_id": 0,
        "P_PARTKEY": 1, "P_BRAND": 1, "P_TYPE": 1, "P_SIZE": 1
    }
)))

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)
supplier_df_string = redis_client.get('supplier')
partsupp_df_string = redis_client.get('partsupp')

# Convert strings to DataFrames
supplier_df = pd.read_json(supplier_df_string, orient='records')
partsupp_df = pd.read_json(partsupp_df_string, orient='records')

# Filter out suppliers with "Customer Complaints" in comments
filtered_supplier_df = supplier_df[
    ~supplier_df.S_COMMENT.str.contains("Customer Complaints")
]

# Merge and filter the DataFrames
merged_df = mongo_part.merge(
    partsupp_df,
    how='inner',
    left_on='P_PARTKEY',
    right_on='PS_PARTKEY'
)
merged_df = merged_df.merge(
    filtered_supplier_df,
    how='inner',
    left_on='PS_SUPPKEY',
    right_on='S_SUPPKEY'
)

# Group and sort the DataFrames
result_df = (
    merged_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])
    .agg(SUPPLIER_CNT=('S_SUPPKEY', 'nunique'))
    .reset_index()
    .sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'],
                 ascending=[False, True, True, True])
)

# Write the result to a CSV file
result_df.to_csv('query_output.csv', index=False)
