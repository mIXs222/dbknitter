# market_share.py

import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_client = DirectRedis(host="redis", port=6379, db=0)

# Load data from Redis
orders_df = pd.read_json(redis_client.get('orders'), orient='records')
lineitem_df = pd.read_json(redis_client.get('lineitem'), orient='records')

# Query MySQL databases
mysql_cursor.execute("SELECT N_NATIONKEY, N_NAME FROM nation")
nations = mysql_cursor.fetchall()

mysql_cursor.execute("SELECT R_REGIONKEY, R_NAME FROM region")
regions = mysql_cursor.fetchall()

# Create DataFrames for MySQL data
nations_df = pd.DataFrame(nations, columns=['N_NATIONKEY', 'N_NAME'])
regions_df = pd.DataFrame(regions, columns=['R_REGIONKEY', 'R_NAME'])

# Find the key of ASIA and INDIA from REGION and NATION
asia_key = regions_df[regions_df['R_NAME'] == 'ASIA']['R_REGIONKEY'].values[0]
india_key = nations_df[nations_df['N_NAME'] == 'INDIA']['N_NATIONKEY'].values[0]

# Load MongoDB collections into DataFrames
supplier_cursor = mongo_db.supplier.find({'S_NATIONKEY': india_key})
customer_cursor = mongo_db.customer.find({'C_NATIONKEY': india_key})

supplier_df = pd.DataFrame(list(supplier_cursor))
customer_df = pd.DataFrame(list(customer_cursor))

# Merge Redis data
merged_df = pd.merge(lineitem_df, orders_df, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merged_df = merged_df[merged_df['L_PARTKEY'].str.contains('SMALL PLATED COPPER')]  # Product Type

# Filter data for the years 1995 and 1996
merged_df['O_YEAR'] = pd.to_datetime(merged_df['O_ORDERDATE']).dt.year
merged_df = merged_df[merged_df['O_YEAR'].isin([1995, 1996])]

# Calculate the revenue for each year
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Group by year and sum the revenues
revenue_per_year_df = merged_df.groupby('O_YEAR')['REVENUE'].sum().reset_index()

# Calculate the total market for ASIA
asia_suppliers = supplier_df['S_SUPPKEY'].tolist()
asia_revenue_per_year_df = merged_df[merged_df['L_SUPPKEY'].isin(asia_suppliers)].groupby('O_YEAR')['REVENUE'].sum().reset_index()
asia_revenue_per_year_df.columns = ['O_YEAR', 'REVENUE_ASIA']

# Calculate market share
market_share_df = pd.merge(asia_revenue_per_year_df, revenue_per_year_df, on='O_YEAR')
market_share_df['MARKET_SHARE'] = market_share_df['REVENUE_ASIA'] / market_share_df['REVENUE']

# Select only the year and market share columns
final_output = market_share_df[['O_YEAR', 'MARKET_SHARE']]

# Write the results to CSV
final_output.to_csv('query_output.csv', index=False)

# Close all connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
