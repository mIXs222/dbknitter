# revenue_analysis.py
import pymongo
import pandas as pd
import direct_redis
from datetime import datetime

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]

# Retrieve data from MongoDB collections
nation_df = pd.DataFrame(list(mongo_db["nation"].find()))
region_df = pd.DataFrame(list(mongo_db["region"].find()))
supplier_df = pd.DataFrame(list(mongo_db["supplier"].find()))

# Redis connection
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Convert Redis data into Pandas DataFrame
customer_df = pd.read_json(redis_client.get('customer'))
orders_df = pd.read_json(redis_client.get('orders'))
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Convert MongoDB field names to conform with the SQL-like structure
nation_df.columns = nation_df.columns.str.replace(r'_', ' ')
region_df.columns = region_df.columns.str.replace(r'_', ' ')
supplier_df.columns = supplier_df.columns.str.replace(r'_', ' ')

# Convert Redis field names to conform with the SQL-like structure
customer_df.columns = customer_df.columns.str.replace(r'_', ' ')
orders_df.columns = orders_df.columns.str.replace(r'_', ' ')
lineitem_df.columns = lineitem_df.columns.str.replace(r'_', ' ')

# Merge operations to simulate SQL joins
asia_region = region_df[region_df['R NAME'] == 'ASIA']
nation_in_asia = nation_df[nation_df['N REGIONKEY'].isin(asia_region['R REGIONKEY'])]

customer_in_asia = customer_df[customer_df['C NATIONKEY'].isin(nation_in_asia['N NATIONKEY'])]
orders_from_asia = orders_df[orders_df['O CUSTKEY'].isin(customer_in_asia['C CUSTKEY'])]

# Date filtering
start_date = datetime(1990, 1, 1)
end_date = datetime(1994, 12, 31)
mask = (orders_from_asia['O ORDERDATE'] >= start_date) & (orders_from_asia['O ORDERDATE'] <= end_date)
orders_from_asia = orders_from_asia.loc[mask]

# Calculate revenue
lineitem_df['revenue'] = lineitem_df['L EXTENDEDPRICE'] * (1 - lineitem_df['L DISCOUNT'])
revenue_by_lineitem = lineitem_df[lineitem_df['L ORDERKEY'].isin(orders_from_asia['O ORDERKEY'])]

# Merge to associate revenue with nations
revenue_with_nations = pd.merge(revenue_by_lineitem, customer_df[['C CUSTKEY', 'C NATIONKEY']], left_on='L SUPPKEY', right_on='C CUSTKEY')
revenue_with_nations = pd.merge(revenue_with_nations, nation_in_asia, left_on='C NATIONKEY', right_on='N NATIONKEY')

# Grouping and summarizing revenue by nation
nation_revenue = revenue_with_nations.groupby('N NAME', as_index=False)['revenue'].sum()

# Sorting and output
nation_revenue_sorted = nation_revenue.sort_values(by='revenue', ascending=False)
nation_revenue_sorted.to_csv('query_output.csv', index=False)
