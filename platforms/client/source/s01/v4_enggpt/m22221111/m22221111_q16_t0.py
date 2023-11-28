# Python code to execute the query on the original data
import pymongo
import direct_redis
import pandas as pd
import re

# Connect to MongoDB
client = pymongo.MongoClient("mongodb", 27017)
mongodb = client["tpch"]
partsupp = pd.DataFrame(list(mongodb["partsupp"].find()))

# Connect to Redis
dr = direct_redis.DirectRedis(host='redis', port=6379, db=0)
part_raw = dr.get('part')
supplier_raw = dr.get('supplier')

# Create DataFrames from Redis raw data
part = pd.read_json(part_raw)
supplier = pd.read_json(supplier_raw)

# Filter both DataFrames according to the criteria
part = part[~part['P_BRAND'].eq('Brand#45')]
part = part[~part['P_TYPE'].str.startswith('MEDIUM POLISHED')]
part = part[part['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9])]
supplier = supplier[~supplier['S_COMMENT'].str.contains('Customer Complaints')]

# Merge DataFrames to combine parts and suppliers
merged_df = partsupp.merge(part, how='inner', left_on='PS_PARTKEY', right_on='P_PARTKEY')
merged_df = merged_df.merge(supplier, how='inner', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Group by brand, type, and size, then count distinct suppliers
results = (merged_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])
            .agg(SUPPLIER_CNT=('PS_SUPPKEY', 'nunique'))
            .reset_index())

# Sort the results as per given conditions
results = results.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Write results to CSV
results.to_csv('query_output.csv', index=False)
