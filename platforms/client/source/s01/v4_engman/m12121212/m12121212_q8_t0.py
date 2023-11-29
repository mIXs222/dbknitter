from pymongo import MongoClient
import pandas as pd
import redis
from direct_redis import DirectRedis


def get_mongodb_data(client, collection_name):
    db = client['tpch']
    collection = db[collection_name]
    return pd.DataFrame(list(collection.find()))

def get_redis_data(r, key):
    data = r.get(key)
    return pd.read_json(data, orient='split')
  
# MongoDB connection
mongo_client = MongoClient('mongodb', 27017)
# Redis connection
redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
dr = DirectRedis(redis_client)

# Fetch data from MongoDB
orders_df = get_mongodb_data(mongo_client, 'orders')
part_df = get_mongodb_data(mongo_client, 'part')
nation_df = get_mongodb_data(mongo_client, 'nation')

# Fetch data from Redis
lineitem_df = get_redis_data(dr, 'lineitem')
supplier_df = get_redis_data(dr, 'supplier')
region_df = get_redis_data(dr, 'region')

# Join and filter the dataframes
merged_df = (
    orders_df.merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    .merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    .merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    .merge(part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')
    .merge(region_df, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
)

# Filter based on conditions
final_df = merged_df[
    (merged_df['P_TYPE'] == 'SMALL PLATED COPPER') &
    (merged_df['R_NAME'] == 'ASIA') &
    (merged_df['N_NAME'] == 'INDIA') &
    merged_df['O_ORDERDATE'].str.contains(r'^199[5-6]')
]

# Calculate revenue
final_df['revenue'] = final_df['L_EXTENDEDPRICE'] * (1 - final_df['L_DISCOUNT'])

# Calculate market share
final_df['year'] = pd.to_datetime(final_df['O_ORDERDATE']).dt.year
market_share_df = final_df.groupby('year')['revenue'].sum().reset_index()

# Rename columns
market_share_df.columns = ['order year', 'market share']

# Write the output to a CSV file
market_share_df.to_csv('query_output.csv', index=False)
