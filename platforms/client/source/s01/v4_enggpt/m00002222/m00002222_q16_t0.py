# query.py

import pandas as pd
import pymysql
import direct_redis
import re

# MySQL connection parameters
mysql_args = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}

# Redis connection parameters
redis_args = {
    'host': 'redis',
    'port': 6379
}

# Connect to MySQL
mysql_conn = pymysql.connect(**mysql_args)

# Define the query for MySQL
mysql_query = """
SELECT p.P_PARTKEY, p.P_NAME, p.P_MFGR, p.P_BRAND, p.P_TYPE, p.P_SIZE, p.P_CONTAINER, p.P_RETAILPRICE, p.P_COMMENT,
       s.S_SUPPKEY, s.S_NAME, s.S_ADDRESS, s.S_NATIONKEY, s.S_PHONE, s.S_ACCTBAL, s.S_COMMENT
FROM part p
JOIN supplier s ON p.P_PARTKEY = s.S_SUPPKEY
WHERE p.P_BRAND != 'Brand#45'
  AND p.P_TYPE NOT LIKE 'MEDIUM POLISHED%'
  AND p.P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
  AND s.S_COMMENT NOT LIKE '%Customer Complaints%'
"""

# Execute the query in MySQL
parts_suppliers = pd.read_sql(mysql_query, mysql_conn)

# Close the MySQL connection
mysql_conn.close()

# Connect to Redis using direct_redis
redis_conn = direct_redis.DirectRedis(**redis_args)

# Get partsupp table from Redis and create pandas DataFrame
partsupp_data = redis_conn.get('partsupp')
partsupp_df = pd.DataFrame([eval(row) for row in partsupp_data.decode('utf-8').strip('[]').split(', ')])

# Perform join on the data from MySQL and Redis
combined_df = partsuppliers.merge(partsupp_df, how='inner', left_on='P_PARTKEY', right_on='PS_PARTKEY')

# Grouping by brand, type, and size
result_df = combined_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']).agg(SUPPLIER_CNT=('S_SUPPKEY', 'nunique')).reset_index()

# Sorting the results
result_df.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True], inplace=True)

# Writing the results to a CSV file
result_df.to_csv('query_output.csv', index=False)
