import pymongo
import pandas as pd
from datetime import datetime
import direct_redis

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]

# Load tables from MongoDB
customers_df = pd.DataFrame(list(mongo_db.customer.find()))
orders_df = pd.DataFrame(list(mongo_db.orders.find()))
lineitem_df = pd.DataFrame(list(mongo_db.lineitem.find()))

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
nation_data = redis_client.get('nation')
nation_df = pd.read_json(nation_data)

# Preprocess the data
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])

# Join and filter data
merged_df = customers_df.merge(orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = merged_df.merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
merged_df = merged_df.merge(nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Apply WHERE conditions
filtered_df = merged_df[
    (merged_df['O_ORDERDATE'] >= datetime(1993, 10, 1)) &
    (merged_df['O_ORDERDATE'] < datetime(1994, 1, 1)) &
    (merged_df['L_RETURNFLAG'] == 'R')
]

# Perform aggregation
result_df = filtered_df.groupby(['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_PHONE', 'N_NAME', 'C_ADDRESS', 'C_COMMENT']).apply(
    lambda df: pd.Series({
        'REVENUE': (df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT'])).sum()
    })
).reset_index()

# Order the result
result_df = result_df.sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[True, True, True, False])

# Write to CSV
result_df.to_csv('query_output.csv', index=False)
