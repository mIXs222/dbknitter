import pymysql
import pymongo
from direct_redis import DirectRedis
import pandas as pd
from datetime import datetime

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
with mysql_conn.cursor() as cursor:
    customer_query = "SELECT C_CUSTKEY, C_NAME, C_NATIONKEY FROM customer;"
    cursor.execute(customer_query)
    customer_data = cursor.fetchall()
    customer_df = pd.DataFrame(customer_data, columns=['C_CUSTKEY', 'C_NAME', 'C_NATIONKEY'])

# MongoDB connection
mongodb_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb_db = mongodb_client["tpch"]
nation_col = mongodb_db["nation"]
supplier_col = mongodb_db["supplier"]
orders_col = mongodb_db["orders"]

# Retrieve documents from MongoDB and create DataFrames
nation_df = pd.DataFrame(list(nation_col.find()))
supplier_df = pd.DataFrame(list(supplier_col.find()))
orders_df = pd.DataFrame(list(orders_col.find(
    {'O_ORDERDATE': {'$gte': datetime(1990, 1, 1), '$lte': datetime(1994, 12, 31)}}
)))

# Redis connection
redis_client = DirectRedis(host='redis', port=6379, db=0)
region_df = pd.read_json(redis_client.get('region').decode("utf-8"))
lineitem_df = pd.read_json(redis_client.get('lineitem').decode("utf-8"))

# Data processing
asia_region_df = region_df[region_df['R_NAME'] == 'ASIA']
asia_nations_df = nation_df[nation_df['N_NATIONKEY'].isin(asia_region_df['R_REGIONKEY'])]

# Merge DataFrames to compute the total revenue
merged_df = pd.merge(customer_df, asia_nations_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
merged_df = pd.merge(merged_df, orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = pd.merge(merged_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
merged_df = pd.merge(merged_df, supplier_df, left_on='C_NATIONKEY', right_on='S_NATIONKEY')

# Calculate revenue
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
nation_revenue = merged_df.groupby('N_NAME')['REVENUE'].sum().reset_index()

# Sort the results
nation_revenue_sorted = nation_revenue.sort_values(by='REVENUE', ascending=False)

# Write to CSV
nation_revenue_sorted.to_csv('query_output.csv', index=False)

# Clean up connections
mysql_conn.close()
mongodb_client.close()
redis_client.close()
