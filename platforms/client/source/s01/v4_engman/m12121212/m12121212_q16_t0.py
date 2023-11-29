# query.py

from pymongo import MongoClient
import pandas as pd
import direct_redis

# MongoDB Connection Setup
mongo_client = MongoClient("mongodb", 27017)
mongo_db = mongo_client['tpch']

# Retrieve MongoDB Documents
part_col = mongo_db['part']
partsupp_col = mongo_db['partsupp']

# Query for the desired parts excluding brand id of 45
part_query = {
    'P_SIZE': {'$in': [49, 14, 23, 45, 19, 3, 36, 9]},
    'P_TYPE': {'$ne': 'MEDIUM POLISHED'},
    'P_BRAND': {'$ne': 'Brand#45'}
}

# Convert MongoDB documents to Pandas DataFrame
part_df = pd.DataFrame(list(part_col.find(part_query, {'_id': 0})))
partsupp_df = pd.DataFrame(list(partsupp_col.find({}, {'_id': 0})))

# Redis Connection Setup
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve Redis Data for supplier
supplier_df = pd.read_msgpack(r.get('supplier'))

# Filter out suppliers with complaints
filtered_supplier_df = supplier_df[~supplier_df['S_COMMENT'].str.contains('Customer Complaints')]

# Merge Tables on part key
joined_df = partsupp_df.merge(part_df, how='inner', left_on='PS_PARTKEY', right_on='P_PARTKEY')

# Merge with supplier data
final_df = joined_df.merge(filtered_supplier_df, how='inner', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Count distinct suppliers
result_df = final_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']).agg({'S_SUPPKEY': 'nunique'}).reset_index()

# Rename columns for final output
result_df.rename(columns={'S_SUPPKEY': 'SUPPLIER_COUNT'}, inplace=True)

# Sort as specified by the query description
result_df.sort_values(by=['SUPPLIER_COUNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True], inplace=True)

# Save to CSV
result_df.to_csv('query_output.csv', index=False)
