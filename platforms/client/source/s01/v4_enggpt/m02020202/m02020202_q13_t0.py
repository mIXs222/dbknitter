# python_code.py

import pymysql
import pandas as pd
import csv
from direct_redis import DirectRedis

# Connect to mysql
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Query orders from MySQL
orders_query = """
    SELECT O_ORDERKEY, O_CUSTKEY, O_COMMENT 
    FROM orders 
    WHERE O_COMMENT NOT LIKE '%pending%' AND O_COMMENT NOT LIKE '%deposits%'
"""
orders_df = pd.read_sql(orders_query, mysql_conn)

# Close the MySQL connection.
mysql_conn.close()

# Connect to Redis
redis = DirectRedis(host='redis', port=6379, db=0)

# Fetch customer data from Redis
customer_df = pd.read_json(redis.get('customer'), orient='records')

# Merge datasets and perform analysis
merged_df = customer_df.merge(orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='left')
merged_df['O_ORDERKEY'] = merged_df['O_ORDERKEY'].notnull().astype(int)  # Treat presence of order key as a count
grouped_df = merged_df.groupby('C_CUSTKEY').agg({'O_ORDERKEY': 'sum'}).reset_index()
grouped_df = grouped_df.rename(columns={'O_ORDERKEY': 'C_COUNT'})
cust_dist_df = grouped_df.groupby('C_COUNT').size().reset_index(name='CUSTDIST')
cust_dist_df.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False], inplace=True)

# Write results to a CSV file
cust_dist_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
