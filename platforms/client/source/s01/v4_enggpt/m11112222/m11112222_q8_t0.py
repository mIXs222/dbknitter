import pandas as pd
from pymongo import MongoClient
import direct_redis
import csv

# Establish a connection to MongoDB
client = MongoClient('mongodb', 27017)
mongodb = client['tpch']

# Query MongoDB for region, nation, supplier and part information
region = pd.DataFrame(list(mongodb.region.find({'R_NAME': 'ASIA'})))
nation = pd.DataFrame(list(mongodb.nation.find({'N_NAME': 'INDIA', 'N_REGIONKEY': {'$in': region['R_REGIONKEY'].tolist()}})))
supplier = pd.DataFrame(list(mongodb.supplier.find({'S_NATIONKEY': {'$in': nation['N_NATIONKEY'].tolist()}})))
part = pd.DataFrame(list(mongodb.part.find({'P_TYPE': 'SMALL PLATED COPPER'})))

# Establish connection to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve and convert Redis data to Pandas DataFrames for customer, orders, lineitem
customer = pd.read_json(r.get('customer').decode('utf-8'), orient='records')
orders = pd.read_json(r.get('orders').decode('utf-8'), orient='records')
lineitem = pd.read_json(r.get('lineitem').decode('utf-8'), orient='records')

# Filter orders between the years 1995 and 1996 and parse O_ORDERDATE
orders['O_ORDERDATE'] = pd.to_datetime(orders['O_ORDERDATE'])
orders_filtered = orders[(orders['O_ORDERDATE'] >= '1995-01-01') & (orders['O_ORDERDATE'] < '1997-01-01')]

# Merge tables on keys
merged_data = lineitem.merge(part, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')
merged_data = merged_data.merge(supplier, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')
merged_data = merged_data.merge(orders_filtered, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merged_data = merged_data.merge(customer, how='inner', left_on='O_CUSTKEY', right_on='C_CUSTKEY')
merged_data = merged_data.merge(nation, how='inner', left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Calculate volume as extended price adjusted by discount
merged_data['VOLUME'] = merged_data['L_EXTENDEDPRICE'] * (1 - merged_data['L_DISCOUNT'])

# Group by 'O_ORDERDATE' (year), calculate volumes for INDIA, and total volume
annual_volume = merged_data.groupby(merged_data['O_ORDERDATE'].dt.year)['VOLUME'].sum().rename('ANNUAL_VOLUME')
india_volume = merged_data[merged_data['N_NAME'] == 'INDIA'].groupby(merged_data['O_ORDERDATE'].dt.year)['VOLUME'].sum().rename('INDIA_VOLUME')

# Calculate market share
market_share = (india_volume / annual_volume).reset_index()
market_share.columns = ['YEAR', 'MARKET_SHARE']

# Write result to CSV file
market_share.sort_values(by='YEAR').to_csv('query_output.csv', index=False)
