import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
customer_collection = mongo_db["customer"]
orders_collection = mongo_db["orders"]
lineitem_collection = mongo_db["lineitem"]

# Query MongoDB
customers = pd.DataFrame(list(customer_collection.find({}, {'_id': 0})))
orders = pd.DataFrame(list(orders_collection.find({}, {'_id': 0})))
lineitems = pd.DataFrame(list(lineitem_collection.find({}, {'_id': 0})))

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Query Redis, assume `get` retrieves a pandas DataFrame
nation = pd.read_msgpack(redis_client.get('nation'))
region = pd.read_msgpack(redis_client.get('region'))
supplier = pd.read_msgpack(redis_client.get('supplier'))

# Filter only ASIA region
asia_region = region[region['R_NAME'] == 'ASIA']
asia_nations = nation[nation["N_REGIONKEY"].isin(asia_region["R_REGIONKEY"])]

# Merge dataframes
combined = pd.merge(customers, orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
combined = pd.merge(combined, lineitems, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Filter dates and discount for extended price
start_date = datetime.strptime('1990-01-01', '%Y-%m-%d')
end_date = datetime.strptime('1994-12-31', '%Y-%m-%d')
combined['O_ORDERDATE'] = pd.to_datetime(combined['O_ORDERDATE'])
combined = combined[(combined['O_ORDERDATE'] >= start_date) & (combined['O_ORDERDATE'] <= end_date)]
combined = combined.merge(asia_nations, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Calculate revenue
combined['revenue'] = combined['L_EXTENDEDPRICE'] * (1 - combined['L_DISCOUNT'])

# Group by nations and calculate total revenue
result = combined.groupby('N_NAME').agg({'revenue': 'sum'}).reset_index()

# Sort by revenue in descending order and select relevant columns
result = result.sort_values(by='revenue', ascending=False)
result = result[['N_NAME', 'revenue']]

# Write to CSV
result.to_csv('query_output.csv', index=False)
