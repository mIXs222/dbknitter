# File: query.py
import pandas as pd
import pymysql
from direct_redis import DirectRedis
import csv

# Connect to MySQL database
mysql_conn = pymysql.connect(database='tpch', user='root', password='my-secret-pw', host='mysql')
cursor = mysql_conn.cursor()

# Extract necessary tables from MySQL
cursor.execute("SELECT * FROM lineitem")
lineitem_df = pd.DataFrame(cursor.fetchall(), columns=[i[0] for i in cursor.description])

cursor.execute("SELECT * FROM orders")
orders_df = pd.DataFrame(cursor.fetchall(), columns=[i[0] for i in cursor.description])

cursor.execute("SELECT * FROM partsupp")
partsupp_df = pd.DataFrame(cursor.fetchall(), columns=[i[0] for i in cursor.description])

mysql_conn.close()

# Connect to Redis database
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Extract necessary tables from Redis
nation_df = pd.read_json(redis_conn.get('nation'))
part_df = pd.read_json(redis_conn.get('part'))
supplier_df = pd.read_json(redis_conn.get('supplier'))

# Filter parts with 'dim' in its name
dim_part_df = part_df[part_df['P_NAME'].str.contains('dim')]

# Merge and compute profit
merged_df = (lineitem_df
             .merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
             .merge(dim_part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')
             .merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
             .merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
             .merge(partsupp_df, left_on=['L_PARTKEY', 'L_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY']))

merged_df['Year'] = pd.to_datetime(merged_df['O_ORDERDATE']).dt.year
merged_df['Profit'] = (merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])) - (merged_df['L_QUANTITY'] * merged_df['PS_SUPPLYCOST'])

# Group by nation and year
grouped_df = merged_df.groupby(['N_NAME', 'Year'])['Profit'].sum().reset_index()

# Sort the results
sorted_df = grouped_df.sort_values(by=['N_NAME', 'Year'], ascending=[True, False])

# Write to CSV file
sorted_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_ALL)
