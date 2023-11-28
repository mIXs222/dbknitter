import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MongoDB connection setup
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
collection_nation = mongo_db["nation"]
collection_part = mongo_db["part"]
collection_orders = mongo_db["orders"]

# Redis connection setup
redis_client = DirectRedis(host="redis", port=6379, db=0)

# Retrieve data from MongoDB
nation = pd.DataFrame(list(collection_nation.find({"N_NAME": "INDIA"})))
orders = pd.DataFrame(list(collection_orders.find({"O_ORDERDATE": {"$gte": "1995-01-01", "$lte": "1996-12-31"}})))
part = pd.DataFrame(list(collection_part.find({"P_TYPE": "SMALL PLATED COPPER"})))

# Retrieve data from Redis
region_data = redis_client.get("region")
supplier_data = redis_client.get("supplier")
customer_data = redis_client.get("customer")
lineitem_data = redis_client.get("lineitem")

# Convert redis data strings to DataFrames
region = pd.read_json(region_data)
supplier = pd.read_json(supplier_data)
customer = pd.read_json(customer_data)
lineitem = pd.read_json(lineitem_data)

# Filter and prepare the data for analysis
region_asia = region[region['R_NAME'] == 'ASIA']
nation['N_NATIONKEY'] = nation['N_NATIONKEY'].astype(int)
nation_india = nation[nation['N_NAME'] == "INDIA"]
supplier_asia = supplier[supplier['S_NATIONKEY'].isin(region_asia['R_REGIONKEY'])]
customer_asia = customer[customer['C_NATIONKEY'].isin(nation_india['N_NATIONKEY'])]

# Merge dataframes on keys to obtain the relevant data
output = (
    lineitem.merge(part, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')
    .merge(supplier_asia, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    .merge(orders, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    .merge(customer_asia, how='inner', left_on='O_CUSTKEY', right_on='C_CUSTKEY')
)

# Calculate the volume and year of the orders
output['YEAR'] = pd.to_datetime(output['O_ORDERDATE']).dt.year
output['VOLUME'] = output['L_EXTENDEDPRICE'] * (1 - output['L_DISCOUNT'])

# Group by year and calculate market share
result = output.groupby('YEAR').agg({'VOLUME': 'sum'}).reset_index()
result_total_volume = result['VOLUME'].sum()
result['MARKET_SHARE'] = result['VOLUME'] / result_total_volume

# Order the results and output to csv
result = result.sort_values('YEAR')
result.to_csv('query_output.csv', index=False)
