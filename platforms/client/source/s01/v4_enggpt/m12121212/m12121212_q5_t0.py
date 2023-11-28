from pymongo import MongoClient
import pandas as pd
import direct_redis
import datetime

# MongoDB connection
client = MongoClient('mongodb', 27017)
db_mongo = client['tpch']

# Redis connection
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from MongoDB
nation_mongo = pd.DataFrame(list(db_mongo.nation.find()))
orders_mongo = pd.DataFrame(list(db_mongo.orders.find()))

# Retrieve data from Redis and convert to DataFrames
region = redis_client.get('region').set_index('R_REGIONKEY')
supplier = redis_client.get('supplier').set_index('S_SUPPKEY')
customer = redis_client.get('customer').set_index('C_CUSTKEY')
lineitem = redis_client.get('lineitem').set_index('L_ORDERKEY')

# Convert data to proper datatypes
orders_mongo['O_ORDERDATE'] = pd.to_datetime(orders_mongo['O_ORDERDATE'])

# Filter orders by date
orders_filtered = orders_mongo[
    (orders_mongo['O_ORDERDATE'] >= datetime.datetime(1990, 1, 1)) &
    (orders_mongo['O_ORDERDATE'] <= datetime.datetime(1994, 12, 31))
]

# Merge data
cust_orders = customer.merge(orders_filtered, how='inner', left_index=True, right_on='O_CUSTKEY')
cust_orders_nation = cust_orders.merge(nation_mongo, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
cust_orders_nation_region = cust_orders_nation.merge(region, left_on='N_REGIONKEY', right_index=True)

# Filter customers by the ASIA region
asia_customers = cust_orders_nation_region[cust_orders_nation_region['R_NAME'] == 'ASIA']

# Merge line items for these orders
asia_orders_lineitems = asia_customers.merge(lineitem, left_on='O_ORDERKEY', right_index=True)

# Calculate extended price with discount
asia_orders_lineitems['TOTAL_REVENUE'] = asia_orders_lineitems['L_EXTENDEDPRICE'] * (1 - asia_orders_lineitems['L_DISCOUNT'])

# Group by nation, summing the revenue and ordering the result
grouped_revenue = asia_orders_lineitems.groupby('N_NAME')['TOTAL_REVENUE'].sum().reset_index()
final_result = grouped_revenue.sort_values(by='TOTAL_REVENUE', ascending=False)

# Write to CSV
final_result.to_csv('query_output.csv', index=False)
