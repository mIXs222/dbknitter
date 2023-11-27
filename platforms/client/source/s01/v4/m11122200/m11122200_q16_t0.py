# query.py
from pymongo import MongoClient
import pandas as pd
from direct_redis import DirectRedis
import csv

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Retrieve `part` data from MongoDB
part_collection = mongo_db['part']
part_data = part_collection.find({
    '$and': [
        {'P_BRAND': {'$ne': 'Brand#45'}},
        {'P_TYPE': {'$not': {'$regex': 'MEDIUM POLISHED.*'}}},
        {'P_SIZE': {'$in': [49, 14, 23, 45, 19, 3, 36, 9]}}
    ]
})
part_df = pd.DataFrame(list(part_data))

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve `supplier` data from Redis
supplier_df = pd.read_json(redis_client.get('supplier'))
# Retrieve `partsupp` data from Redis
partsupp_df = pd.read_json(redis_client.get('partsupp'))

# Filter out suppliers based on comment
excluded_suppliers = supplier_df[supplier_df['S_COMMENT'].str.contains('Customer.*Complaints')]['S_SUPPKEY'].tolist()

# Filter partsupp based on excluded suppliers
partsupp_filtered = partsupp_df[~partsupp_df['PS_SUPPKEY'].isin(excluded_suppliers)]

# Merge part and partsupp dataframes on P_PARTKEY and PS_PARTKEY
merged_df = pd.merge(part_df, partsupp_filtered, how="inner", left_on="P_PARTKEY", right_on="PS_PARTKEY")

# Group by P_BRAND, P_TYPE, P_SIZE and count distinct PS_SUPPKEY
result_df = merged_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']) \
    .agg(SUPPLIER_CNT=('PS_SUPPKEY', 'nunique')) \
    .reset_index() \
    .sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Output the result to a CSV file
result_df.to_csv('query_output.csv', index=False)
