import pandas as pd
from direct_redis import DirectRedis
import csv

# Connect to the Redis database
redis_host = "redis"
redis_port = 6379
redis_db = 0
redis = DirectRedis(host=redis_host, port=redis_port, db=redis_db)

# Load data from Redis into Pandas DataFrames
part_df = pd.read_msgpack(redis.get('part'))
supplier_df = pd.read_msgpack(redis.get('supplier'))
partsupp_df = pd.read_msgpack(redis.get('partsupp'))

# Filtering part data
part_df_filtered = part_df[
    ~part_df['P_BRAND'].eq('Brand#45') &
    ~part_df['P_TYPE'].str.startswith('MEDIUM POLISHED') &
    part_df['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9])
]

# Filtering supplier data
supplier_df_filtered = supplier_df[
    ~supplier_df['S_COMMENT'].str.contains('Customer Complaints')
]

# Merging partsupp with filtered part and supplier data
merged_df = partsupp_df.merge(part_df_filtered, how='inner', left_on='PS_PARTKEY', right_on='P_PARTKEY')
merged_df = merged_df.merge(supplier_df_filtered, how='inner', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Grouping by P_BRAND, P_TYPE, P_SIZE and counting distinct suppliers
grouped_df = merged_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])['S_SUPPKEY'].nunique().reset_index(name='SUPPLIER_CNT')

# Sorting the results
sorted_grouped_df = grouped_df.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Write the final dataframe to CSV
sorted_grouped_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
