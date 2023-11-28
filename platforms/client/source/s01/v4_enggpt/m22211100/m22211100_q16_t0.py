import pandas as pd
from pymongo import MongoClient
import direct_redis
import csv

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
mongo_db = client['tpch']
supplier_col = mongo_db['supplier']
partsupp_col = mongo_db['partsupp']

# Fetch data from MongoDB
suppliers = pd.DataFrame(list(supplier_col.find()))
partsupp = pd.DataFrame(list(partsupp_col.find()))

# Filter out suppliers with specific comments
suppliers = suppliers[~suppliers['S_COMMENT'].str.contains('Customer Complaints')]

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch part dataframe from Redis
part = redis_client.get('part')

# Merge partsupp and part on keys and apply filters
merged_df = (
    partsupp.merge(suppliers, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
    .merge(part, left_on='PS_PARTKEY', right_on='P_PARTKEY')
)

# Apply filters on part attributes
filtered_df = merged_df[
    (~merged_df['P_BRAND'].eq('Brand#45')) &
    (~merged_df['P_TYPE'].str.startswith('MEDIUM POLISHED')) &
    (merged_df['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9]))
]

# Group by brand, type, size and count distinct suppliers
grouped_df = (
    filtered_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])
    .agg(SUPPLIER_CNT=pd.NamedAgg(column='S_SUPPKEY', aggfunc="nunique"))
    .reset_index()
)

# Order the results
ordered_df = grouped_df.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Write to csv
ordered_df.to_csv('query_output.csv', index=False)

# Close the connections
client.close()
