import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
# Tables in MongoDB
region_table = pd.DataFrame(list(mongo_db["region"].find()))
supplier_table = pd.DataFrame(list(mongo_db["supplier"].find()))
customer_table = pd.DataFrame(list(mongo_db["customer"].find()))
lineitem_table = pd.DataFrame(list(mongo_db["lineitem"].find()))

# Filter Asia region
asia_region_keys = region_table[region_table['R_NAME'] == 'ASIA']['R_REGIONKEY'].tolist()
asia_nation_keys = supplier_table[supplier_table['S_NATIONKEY'].isin(asia_region_keys)]['S_NATIONKEY'].tolist()

# Filter Asia customers
asia_customers = customer_table[customer_table['C_NATIONKEY'].isin(asia_nation_keys)]

# Redis connection
redis_client = DirectRedis(host='redis', port=6379, db=0)
# Tables in Redis
nation_table = pd.read_json(redis_client.get('nation'))
orders_table = pd.read_json(redis_client.get('orders'))

# Convert date strings to datetime
orders_table['O_ORDERDATE'] = pd.to_datetime(orders_table['O_ORDERDATE'])

# Filter orders between 1990 and 1994
start_date = datetime(1990, 1, 1)
end_date = datetime(1994, 12, 31)
filtered_orders = orders_table[(orders_table['O_ORDERDATE'] >= start_date) & (orders_table['O_ORDERDATE'] <= end_date)]
# Filter orders from Asia customers
filtered_orders = filtered_orders[filtered_orders['O_CUSTKEY'].isin(asia_customers['C_CUSTKEY'].tolist())]

# Merge operations to join tables
result = pd.merge(filtered_orders, asia_customers, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
result = pd.merge(result, lineitem_table, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
result = pd.merge(result, nation_table, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Calculate revenue
result['REVENUE'] = result['L_EXTENDEDPRICE'] * (1 - result['L_DISCOUNT'])

# Group by nation and calculate total revenue
revenue_by_nation = result.groupby('N_NAME')['REVENUE'].sum().reset_index()
# Sort by revenue in descending order
revenue_by_nation = revenue_by_nation.sort_values(by='REVENUE', ascending=False)

# Write result to CSV
revenue_by_nation.to_csv('query_output.csv', index=False)
