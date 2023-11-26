import pymongo
import pandas as pd
from datetime import datetime
import direct_redis

# Define constants for connecting to the MongoDB and Redis instances
MONGO_DB_NAME = 'tpch'
MONGO_PORT = 27017
MONGO_HOSTNAME = 'mongodb'

REDIS_DB_NAME = 0
REDIS_PORT = 6379
REDIS_HOSTNAME = 'redis'

# Create a connection to the MongoDB database
mongo_client = pymongo.MongoClient(host=MONGO_HOSTNAME, port=MONGO_PORT)
mongo_db = mongo_client[MONGO_DB_NAME]
mongo_customer = pd.DataFrame(list(mongo_db.customer.find()))
mongo_orders = pd.DataFrame(list(mongo_db.orders.find()))
mongo_lineitem = pd.DataFrame(list(mongo_db.lineitem.find()))

# Create a connection to the Redis database
r = direct_redis.DirectRedis(host=REDIS_HOSTNAME, port=REDIS_PORT, db=REDIS_DB_NAME)
nation = pd.read_msgpack(r.get('nation'))
region = pd.read_msgpack(r.get('region'))
part = pd.read_msgpack(r.get('part'))
supplier = pd.read_msgpack(r.get('supplier'))

# Merge MongoDB and Redis DataFrames to simulate the SQL joins
merged_df = mongo_lineitem.merge(mongo_orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merged_df = merged_df.merge(mongo_customer, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
merged_df = merged_df.merge(supplier, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
merged_df = merged_df.merge(part, left_on='L_PARTKEY', right_on='P_PARTKEY')
merged_df = merged_df.merge(nation.rename(columns={'N_NATIONKEY': 'C_NATIONKEY'}), on='C_NATIONKEY')
merged_df = merged_df.merge(region, left_on='N_REGIONKEY', right_on='R_REGIONKEY')

# Filter data as per the WHERE conditions
asia_customers = (
    merged_df[
        (merged_df['R_NAME'] == 'ASIA') &
        (merged_df['O_ORDERDATE'] >= datetime(1995, 1, 1)) &
        (merged_df['O_ORDERDATE'] <= datetime(1996, 12, 31)) &
        (merged_df['P_TYPE'] == 'SMALL PLATED COPPER')
    ]
)

# Calculate the required columns
asia_customers['VOLUME'] = asia_customers['L_EXTENDEDPRICE'] * (1 - asia_customers['L_DISCOUNT'])
asia_customers['O_YEAR'] = asia_customers['O_ORDERDATE'].dt.year
asia_customers['NATION'] = asia_customers['N_NAME']

# Group by and aggregate data to obtain the result
result = asia_customers.groupby('O_YEAR').apply(
    lambda x: pd.Series({
        'MKT_SHARE': (sum(x[x['NATION'] == 'INDIA']['VOLUME']) / sum(x['VOLUME']))
    })
).reset_index()

# Write the result to a CSV file
result.to_csv('query_output.csv', index=False)
