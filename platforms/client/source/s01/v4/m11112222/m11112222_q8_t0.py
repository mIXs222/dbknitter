import pymongo
import pandas as pd
from datetime import datetime
import direct_redis

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]

# Redis connection
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Load data from MongoDB
nation = pd.DataFrame(list(mongo_db.nation.find({}, {'_id': 0})))
region = pd.DataFrame(list(mongo_db.region.find({}, {'_id': 0})))
supplier = pd.DataFrame(list(mongo_db.supplier.find({}, {'_id': 0})))
part = pd.DataFrame(list(mongo_db.part.find({}, {'_id': 0})))

# Load data from Redis
customer_df = redis_client.get('customer')
orders_df = redis_client.get('orders')
lineitem_df = redis_client.get('lineitem')

# Convert Redis strings to Pandas DataFrames
customer = pd.read_json(customer_df)
orders = pd.read_json(orders_df)
lineitem = pd.read_json(lineitem_df)

# Begin processing the equivalent of the SQL query
asia_nation_keys = nation[nation['N_REGIONKEY'].isin(region[region['R_NAME'] == 'ASIA']['R_REGIONKEY'])]['N_NATIONKEY'].tolist()
asia_customers = customer[customer['C_NATIONKEY'].isin(asia_nation_keys)]
asia_orders = orders[(orders['O_ORDERDATE'] >= datetime(1995, 1, 1)) & (orders['O_ORDERDATE'] <= datetime(1996, 12, 31))]

# Filter necessary parts
parts_filtered = part[part['P_TYPE'] == 'SMALL PLATED COPPER']

# Merge data
merged_df = pd.merge(lineitem, asia_orders, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merged_df = pd.merge(merged_df, parts_filtered, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')
merged_df = pd.merge(merged_df, supplier, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')
merged_df = pd.merge(merged_df, asia_customers, how='inner', left_on='O_CUSTKEY', right_on='C_CUSTKEY')
merged_df = pd.merge(merged_df, nation, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Calculate result
merged_df['O_YEAR'] = merged_df['O_ORDERDATE'].dt.year
merged_df['VOLUME'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

result_df = merged_df.groupby('O_YEAR').apply(
    lambda x: pd.Series({
        'MKT_SHARE': x[x['N_NAME'] == 'INDIA']['VOLUME'].sum() / x['VOLUME'].sum()
    })
).reset_index()

# Write to file
result_df.to_csv('query_output.csv', index=False)
