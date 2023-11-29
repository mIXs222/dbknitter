import pymongo
import pandas as pd
import redis
import direct_redis
from pandas import DataFrame

# MongoDB connection and query
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
parts_table = mongo_db['partsupp']

partsupp_df = pd.DataFrame(list(parts_table.find()))
partsupp_df.columns = ['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT']

# Redis connection and query
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
nation_df = DataFrame(redis_client.get('nation'))
supplier_df = DataFrame(redis_client.get('supplier'))

nation_df.columns = ['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT']
supplier_df.columns = ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT']

# Merge dataframes
df_merge = partsupp_df.merge(supplier_df, how='inner', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
df_merge = df_merge.merge(nation_df, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Filter for German suppliers
german_suppliers_df = df_merge[df_merge['N_NAME'] == 'GERMANY']

# Calculate value
german_suppliers_df['VALUE'] = german_suppliers_df['PS_AVAILQTY'] * german_suppliers_df['PS_SUPPLYCOST']

# Select significant stock
significant_stock_df = german_suppliers_df[german_suppliers_df['VALUE'] > german_suppliers_df['VALUE'].sum() * 0.0001]

# Final results ordered by value
important_subset_df = significant_stock_df[['PS_PARTKEY', 'VALUE']].sort_values(by='VALUE', ascending=False)

# Write to CSV
important_subset_df.to_csv('query_output.csv', index=False)
