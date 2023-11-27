import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connection to MongoDB
client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = client["tpch"]

# Connection to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Load data from MongoDB
nation_df = pd.DataFrame(list(mongodb.nation.find()))
supplier_df = pd.DataFrame(list(mongodb.supplier.find()))

# Load data from Redis
partsupp_df = pd.read_json(redis_client.get('partsupp'), orient='records')
lineitem_df = pd.read_json(redis_client.get('lineitem'), orient='records')

# Prepare the specified dimension filter for part names
specified_dim = 'SPECIFIED_DIM'

# Data Transformation
# - Merge data from different sources
# - Calculate profit per line
merged_df = (lineitem_df
             .merge(partsupp_df, left_on=['L_PARTKEY', 'L_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY'])
             .merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
             .merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY'))

# Apply the filter for part names containing the specified dimension
merged_df = merged_df[merged_df['P_NAME'].str.contains(specified_dim)]

# Calculate profit
merged_df['profit'] = (merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT']) - 
                       merged_df['PS_SUPPLYCOST'] * merged_df['L_QUANTITY'])

# Extract year from order date
merged_df['year'] = pd.to_datetime(merged_df['O_ORDERDATE']).dt.year

# Group by nation and year, and calculate the total profit
result_df = merged_df.groupby(['N_NAME', 'year'])['profit'].sum().reset_index()

# Sort the results as per the query requirements
result_df.sort_values(by=['N_NAME', 'year'], ascending=[True, False], inplace=True)

# Save to CSV file
result_df.to_csv('query_output.csv', index=False)
