from pymongo import MongoClient
import pandas as pd

# MongoDB connection
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Getting data from MongoDB
supplier_df = pd.DataFrame(list(mongo_db.supplier.find(
    {}, {"_id": 0, "S_SUPPKEY": 1, "S_COMMENT": 1})))

partsupp_df = pd.DataFrame(list(mongo_db.partsupp.find(
    {}, {"_id": 0, "PS_SUPPKEY": 1, "PS_PARTKEY": 1})))

# Filter out suppliers with complaints from 'S_COMMENT'.
supplier_df = supplier_df[~supplier_df['S_COMMENT'].str.contains('Customer Complaints')]

# Redis connection and data retrieval
import direct_redis
r = direct_redis.DirectRedis(host='redis', port=6379)
part_df = pd.read_json(r.get('part'), orient='records')

# Filtering part dataframe based on conditions
filtered_part_df = part_df[
    (part_df['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9])) &
    (~part_df['P_TYPE'].str.contains('MEDIUM POLISHED')) &
    (part_df['P_BRAND'] != 'Brand#45')
]

# Combine parts from Redis with partsupp and supplier from MongoDB
combined_df = filtered_part_df.merge(partsupp_df, left_on='P_PARTKEY', right_on='PS_PARTKEY')
combined_df = combined_df.merge(supplier_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Make the final result
result = combined_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']).agg({'S_SUPPKEY': 'count'}).reset_index()
result.rename(columns={'S_SUPPKEY': 'supplier_count'}, inplace=True)

# Sort the result
result.sort_values(by=['supplier_count', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True], inplace=True)

# Write result to CSV
result.to_csv('query_output.csv', index=False)
