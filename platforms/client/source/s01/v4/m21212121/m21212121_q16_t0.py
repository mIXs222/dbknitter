# query.py
import pymongo
import redis
import pandas as pd
from direct_redis import DirectRedis
from io import StringIO

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
mongo_supplier = mongo_db["supplier"]

# Fetch data from MongoDB supplier table
supplier_data = list(mongo_supplier.find({}, {
    "_id": 0,
    "S_SUPPKEY": 1,
    "S_COMMENT": 1
}))

# Filter out suppliers with specific comments
excluded_suppliers = set(
    doc['S_SUPPKEY']
    for doc in supplier_data
    if 'Customer' in doc['S_COMMENT'] and 'Complaints' in doc['S_COMMENT']
)

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Fetch and convert data from Redis part table
part_data_csv = redis_client.get('part').decode('utf-8')
part_data_df = pd.read_csv(StringIO(part_data_csv))

# Fetch and convert data from Redis partsupp table
partsupp_data_csv = redis_client.get('partsupp').decode('utf-8')
partsupp_data_df = pd.read_csv(StringIO(partsupp_data_csv))

# Filter partsupp entries according to supplier condition
partsupp_filtered_df = partsupp_data_df[~partsupp_data_df['PS_SUPPKEY'].isin(excluded_suppliers)]

# Merge datasets and execute the query logic
merged_df = pd.merge(part_data_df, partsupp_filtered_df, left_on='P_PARTKEY', right_on='PS_PARTKEY')

# Apply filters to the merged dataset
filtered_df = merged_df[
    (merged_df['P_BRAND'] != 'Brand#45') &
    (~merged_df['P_TYPE'].str.startswith('MEDIUM POLISHED')) &
    (merged_df['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9]))
]

# Group by and count distinct suppliers
result_df = filtered_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']).agg({'PS_SUPPKEY': pd.Series.nunique})
result_df = result_df.rename(columns={'PS_SUPPKEY': 'SUPPLIER_CNT'})
result_df = result_df.reset_index()

# Sort as per the given order
result_df = result_df.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Write results to CSV
result_df.to_csv('query_output.csv', index=False) 
