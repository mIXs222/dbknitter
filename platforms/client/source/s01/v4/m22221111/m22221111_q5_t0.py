import pymongo
import pandas as pd
from datetime import datetime
import direct_redis

# Connect to MongoDB
client = pymongo.MongoClient("mongodb", 27017)
db = client["tpch"]

# Get MongoDB data
customer_df = pd.DataFrame(list(db.customer.find()))
orders_df = pd.DataFrame(list(db.orders.find()))
lineitem_df = pd.DataFrame(list(db.lineitem.find()))

# Filter the orders by the given date range
orders_df = orders_df[(orders_df['O_ORDERDATE'] >= '1990-01-01') & 
                      (orders_df['O_ORDERDATE'] < '1995-01-01')]

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get Redis data
nation_df = pd.read_json(r.get('nation'))
region_df = pd.read_json(r.get('region'))
supplier_df = pd.read_json(r.get('supplier'))

# Perform joins
merged_df = lineitem_df.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merged_df = merged_df.merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
merged_df = merged_df.merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
merged_df = merged_df.merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
merged_df = merged_df.merge(region_df, left_on='N_REGIONKEY', right_on='R_REGIONKEY')

# Filter Asia region
asia_df = merged_df[merged_df['R_NAME'] == 'ASIA']

# Calculate revenue
asia_df['REVENUE'] = asia_df['L_EXTENDEDPRICE'] * (1 - asia_df['L_DISCOUNT'])

# Group and order results
query_results = asia_df.groupby('N_NAME').agg({'REVENUE': 'sum'}).reset_index()
query_results = query_results.sort_values('REVENUE', ascending=False)

# Writing output to CSV file
query_results.to_csv('query_output.csv', index=False)
