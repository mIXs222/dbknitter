# query.py
import pandas as pd
import pymysql
from datetime import datetime
import direct_redis

# Create a connection to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch')

# Query to retrieve orders within the date range
lineitem_query = """
SELECT
    L_SUPPKEY AS SUPPLIER_NO,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS TOTAL_REVENUE
FROM
    lineitem
WHERE
    L_SHIPDATE BETWEEN '1996-01-01' AND '1996-03-31'
GROUP BY
    L_SUPPKEY;
"""

try:
    lineitem_revenue = pd.read_sql(lineitem_query, mysql_connection)
finally:
    mysql_connection.close()

# Connect to Redis
redis_connection = direct_redis.DirectRedis(host="redis", port=6379, db=0)

# Retrieve the 'supplier' key-value as pandas DataFrame
supplier_data = pd.read_json(redis_connection.get('supplier'))

# Merge the data from MySQL and Redis
merged_data = pd.merge(supplier_data, lineitem_revenue, how='inner', left_on='S_SUPPKEY', right_on='SUPPLIER_NO')

# Find the supplier with the maximum total revenue
max_revenue_supplier = merged_data[merged_data['TOTAL_REVENUE'] == merged_data['TOTAL_REVENUE'].max()]

# Sort by supplier key (ascending) and write to CSV
max_revenue_supplier.sort_values('S_SUPPKEY', ascending=True).to_csv('query_output.csv', index=False)
