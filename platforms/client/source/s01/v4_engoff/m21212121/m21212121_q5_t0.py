from pymongo import MongoClient
import pandas as pd
from direct_redis import DirectRedis

# Connect to the MongoDB
client = MongoClient('mongodb', 27017)
mongo_db = client['tpch']

# Load collections from MongoDB
region = pd.DataFrame(list(mongo_db.region.find()))
supplier = pd.DataFrame(list(mongo_db.supplier.find()))
customer = pd.DataFrame(list(mongo_db.customer.find()))
lineitem = pd.DataFrame(list(mongo_db.lineitem.find()))

# Filter Asia region
asia_region_keys = region[region['R_NAME'] == 'ASIA']['R_REGIONKEY'].tolist()

# Filter suppliers in Asia
asia_supplier_keys = supplier[supplier['S_NATIONKEY'].isin(asia_region_keys)]['S_SUPPKEY'].tolist()

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Load data from Redis
nation = pd.read_json(redis_client.get('nation'))
orders = pd.read_json(redis_client.get('orders'))

# Filter nations in Asia
asia_nation_keys = nation[nation['N_REGIONKEY'].isin(asia_region_keys)]['N_NATIONKEY'].tolist()

# Filter customers in Asia
asia_customers = customer[customer['C_NATIONKEY'].isin(asia_nation_keys)]

# Filter orders between 1990 and 1995
orders['O_ORDERDATE'] = pd.to_datetime(orders['O_ORDERDATE'])
orders_filtered = orders[(orders['O_ORDERDATE'] >= pd.Timestamp('1990-01-01')) & (orders['O_ORDERDATE'] < pd.Timestamp('1995-01-01'))]

# Merge dataframes to get the final dataset to calculate revenue
results = pd.merge(orders_filtered, asia_customers, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
results = pd.merge(results, lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
results = results[results['L_SUPPKEY'].isin(asia_supplier_keys)]

# Calculate revenue
results['REVENUE'] = results['L_EXTENDEDPRICE'] * (1 - results['L_DISCOUNT'])

# Group by nations and sum revenue
final_results = results.groupby('C_NATIONKEY').agg({'REVENUE': 'sum'})

# Join with nation names
final_results = final_results.join(nation.set_index('N_NATIONKEY')['N_NAME']).reset_index()

# Order results by revenue
final_results = final_results.sort_values(by='REVENUE', ascending=False)

# Get only needed columns
final_results = final_results[['N_NAME', 'REVENUE']]

# Write to CSV
final_results.to_csv('query_output.csv', index=False)
