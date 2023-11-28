import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
partsupp_col = mongo_db["partsupp"]
orders_col = mongo_db["orders"]
lineitem_col = mongo_db["lineitem"]

# Redis connection
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieving data from Redis
def get_redis_data(table_name):
    data = redis_client.get(table_name)
    if data:
        return pd.read_json(data, orient='records')
    else:
        return pd.DataFrame()

nation_df = get_redis_data('nation')
part_df = get_redis_data('part')
supplier_df = get_redis_data('supplier')

# Filter 'part' dataframe to include parts with 'dim' in the name
part_df_dim = part_df[part_df['P_NAME'].str.contains('dim', case=False)]

# Get data from MongoDB (Convert cursor to DataFrame)
partsupp_df = pd.DataFrame(list(partsupp_col.find()))
orders_df = pd.DataFrame(list(orders_col.find()))
lineitem_df = pd.DataFrame(list(lineitem_col.find()))

# Convert dates to datetime objects for consistency
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])

# Merge DataFrames
merged_df = (lineitem_df
             .merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
             .merge(partsupp_df, on=['L_PARTKEY', 'L_SUPPKEY'])
             .merge(part_df_dim, left_on='L_PARTKEY', right_on='P_PARTKEY')
             .merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
             .merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY'))

# Calculate profit for each line item
merged_df['PROFIT'] = (merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])) - (merged_df['PS_SUPPLYCOST'] * merged_df['L_QUANTITY'])

# Group by nation and year
result_df = (merged_df
             .groupby([merged_df['N_NAME'], merged_df['O_ORDERDATE'].dt.year])['PROFIT']
             .sum()
             .reset_index()
             .rename(columns={'N_NAME': 'NATION', 'O_ORDERDATE': 'YEAR'}))

# Sorting result based on specifications
result_df.sort_values(by=['NATION', 'YEAR'], ascending=[True, False], inplace=True)

# Write to CSV
result_df.to_csv('query_output.csv', index=False)
