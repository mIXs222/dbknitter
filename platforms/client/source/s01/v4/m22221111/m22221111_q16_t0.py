from pymongo import MongoClient
import pandas as pd
import redis
import direct_redis

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
partsupp_collection = mongo_db['partsupp']

# Query partsupp data from MongoDB
partsupp_df = pd.DataFrame(list(partsupp_collection.find({}, {'_id': 0, 'PS_PARTKEY': 1, 'PS_SUPPKEY': 1})))

# Connect to Redis with direct_redis to use the Pandas extension
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
part_df = redis_client.get('part')  # This should be converted from JSON or similar format
supplier_df = redis_client.get('supplier')  # This should be converted from JSON or similar format

# Filtering for the suppliers that do not have complaints
suppliers_with_complaints = supplier_df[supplier_df['S_COMMENT'].str.contains('Customer%Complaints%')]
valid_suppliers = supplier_df[~supplier_df['S_SUPPKEY'].isin(suppliers_with_complaints['S_SUPPKEY'])]

# Merge the part and partsupp dataframes on key and apply the SQL query conditions
merged_df = pd.merge(part_df, partsupp_df, left_on='P_PARTKEY', right_on='PS_PARTKEY')
filtered_df = merged_df[
    (~merged_df['P_BRAND'].eq('Brand#45')) &
    (~merged_df['P_TYPE'].str.startswith('MEDIUM POLISHED')) &
    (merged_df['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9]))
]

# Filter out invalid suppliers
filtered_df = filtered_df[filtered_df['PS_SUPPKEY'].isin(valid_suppliers['S_SUPPKEY'])]

# Group by the required fields and aggregate using COUNT DISTINCT
grouped_df = filtered_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']).agg(SUPPLIER_CNT=('PS_SUPPKEY', pd.Series.nunique))

# Reset index to make the group by columns as normal columns, and then sort the result as required
result_df = grouped_df.reset_index().sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Write the result to a CSV file
result_df.to_csv('query_output.csv', index=False)
