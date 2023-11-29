import pymongo
import pandas as pd
from datetime import datetime
import direct_redis

# Mongo connection and query
client = pymongo.MongoClient('mongodb', 27017)
mongo_db = client['tpch']
lineitem_df = pd.DataFrame(list(mongo_db.lineitem.find(
    {},
    {'_id': 0, 'L_SUPPKEY': 1, 'L_PARTKEY': 1, 'L_EXTENDEDPRICE': 1,
     'L_DISCOUNT': 1, 'L_QUANTITY': 1, 'L_SHIPDATE': 1})))
supplier_df = pd.DataFrame(list(mongo_db.supplier.find(
    {},
    {'_id': 0, 'S_SUPPKEY': 1, 'S_NATIONKEY': 1})))

# Redis connection and query
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
nation_str = redis_client.get('nation')
part_str = redis_client.get('part')
partsupp_str = redis_client.get('partsupp')

nation_df = pd.read_json(nation_str)
part_df = pd.read_json(part_str)
partsupp_df = pd.read_json(partsupp_str)

# Filtering lineitems by parts containing the specified "dim" in their names
specified_dim = 'dim'  # this should be replaced with the actual value you are interested in
part_df_filtered = part_df[part_df['P_NAME'].str.contains(specified_dim, regex=False)]

# Merging dataframes to get all necessary information for profit calculation
merged_df = lineitem_df.merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
merged_df = merged_df.merge(part_df_filtered, left_on='L_PARTKEY', right_on='P_PARTKEY')
merged_df = merged_df.merge(partsupp_df, on=['PS_PARTKEY', 'PS_SUPPKEY'])

# Adding year from L_SHIPDATE (assuming L_SHIPDATE is formatted as YYYY-MM-DD)
merged_df['year'] = pd.to_datetime(merged_df['L_SHIPDATE']).dt.year

# Calculate profit
merged_df['profit'] = (merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])) - (merged_df['PS_SUPPLYCOST'] * merged_df['L_QUANTITY'])

# Group by nation and year, then sort and aggregate profit
result_df = (merged_df.groupby(['N_NAME', 'year'])
             .agg({'profit': 'sum'})
             .reset_index()
             .sort_values(['N_NAME', 'year'], ascending=[True, False]))

# Write to CSV
result_df.to_csv('query_output.csv', index=False)
