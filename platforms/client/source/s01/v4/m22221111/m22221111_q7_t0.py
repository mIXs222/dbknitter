import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# MongoDB connection
client_mongo = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = client_mongo["tpch"]

# Load the required collections from MongoDB
customer_df = pd.DataFrame(list(mongo_db["customer"].find()))
orders_df = pd.DataFrame(list(mongo_db["orders"].find()))
lineitem_df = pd.DataFrame(list(mongo_db["lineitem"].find()))

# Redis connection
redis_conn = DirectRedis(host="redis", port=6379, db=0)

# Load the required keys from Redis
nation_df = pd.read_json(redis_conn.get('nation'))
supplier_df = pd.read_json(redis_conn.get('supplier'))

# Transform date from string to datetime for comparison
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'], format='%Y-%m-%d')

# Filter lineitem dates
lineitem_df = lineitem_df[(lineitem_df['L_SHIPDATE'] >= datetime(1995, 1, 1)) &
                          (lineitem_df['L_SHIPDATE'] <= datetime(1996, 12, 31))]

# Merge the dataframes
merged_df = (
    supplier_df.merge(lineitem_df, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
    .merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    .merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
    .merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    .merge(nation_df.rename(columns={'N_NATIONKEY': 'C_NATIONKEY', 'N_NAME': 'C_NATION'}), 
           on='C_NATIONKEY')
)

# Filter for nations India and Japan
filtered_df = merged_df[
    ((merged_df['N_NAME'] == 'JAPAN') & (merged_df['C_NATION'] == 'INDIA')) |
    ((merged_df['N_NAME'] == 'INDIA') & (merged_df['C_NATION'] == 'JAPAN'))
]

# Calculate volume and year
filtered_df['VOLUME'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])
filtered_df['L_YEAR'] = filtered_df['L_SHIPDATE'].dt.year

# Group by
result = (
    filtered_df.groupby(['N_NAME', 'C_NATION', 'L_YEAR'])
    .agg(REVENUE=pd.NamedAgg(column='VOLUME', aggfunc='sum'))
    .reset_index()
    .rename(columns={'N_NAME': 'SUPP_NATION', 'C_NATION': 'CUST_NATION'})
    .sort_values(['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])
)

# Write to csv
result.to_csv('query_output.csv', index=False)

# Clean up the connection
client_mongo.close()
redis_conn.close()
