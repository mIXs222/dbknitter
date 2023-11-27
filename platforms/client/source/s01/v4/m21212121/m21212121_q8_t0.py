import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]

# Redis connection
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Fetch data from MongoDB
region_df = pd.DataFrame(list(mongo_db.region.find({}, {'_id': 0})))
supplier_df = pd.DataFrame(list(mongo_db.supplier.find({}, {'_id': 0})))
customer_df = pd.DataFrame(list(mongo_db.customer.find({}, {'_id': 0})))
lineitem_df = pd.DataFrame(list(mongo_db.lineitem.find({}, {'_id': 0})))

# Fetch data from Redis
nation_df = pd.read_msgpack(redis_client.get('nation'))
part_df = pd.read_msgpack(redis_client.get('part'))
orders_df = pd.read_msgpack(redis_client.get('orders'))

# Filter by conditions including date and P_TYPE
filtered_orders = orders_df[
    (pd.to_datetime(orders_df['O_ORDERDATE']) >= datetime(1995, 1, 1)) &
    (pd.to_datetime(orders_df['O_ORDERDATE']) <= datetime(1996, 12, 31))
]

filtered_parts = part_df[part_df['P_TYPE'] == 'SMALL PLATED COPPER']

# Merge dataframes
merged_df = (
    lineitem_df
    .merge(filtered_orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    .merge(filtered_parts, left_on='L_PARTKEY', right_on='P_PARTKEY')
    .merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    .merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
    .merge(nation_df.rename(columns={'N_NATIONKEY': 'C_NATIONKEY', 'N_NAME': 'N1_NAME'}), on='C_NATIONKEY')
    .merge(region_df, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
    .merge(nation_df.rename(columns={'N_NATIONKEY': 'S_NATIONKEY', 'N_NAME': 'N2_NAME'}), on='S_NATIONKEY')
)
merged_df = merged_df[merged_df['R_NAME'] == 'ASIA']

# Calculate VOLUME
merged_df['VOLUME'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Group by YEAR
merged_df['O_YEAR'] = pd.to_datetime(merged_df['O_ORDERDATE']).dt.year
result = merged_df.groupby('O_YEAR').apply(
    lambda x: pd.Series({
        'MKT_SHARE': sum(x['VOLUME'][x['N2_NAME'] == 'INDIA']) / sum(x['VOLUME'])
    })
)

result.to_csv('query_output.csv')
