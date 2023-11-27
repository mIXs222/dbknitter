# python_code.py

import pymysql
import pandas as pd
import direct_redis
import csv

# MySQL connection setup
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Query to fetch lineitem table data
lineitem_query = """
SELECT
  L_SUPPKEY AS SUPPLIER_NO,
  SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS TOTAL_REVENUE
FROM
  lineitem
WHERE
  L_SHIPDATE >= '1996-01-01'
  AND L_SHIPDATE < DATE_ADD('1996-01-01', INTERVAL 3 MONTH)
GROUP BY
  L_SUPPKEY
"""

# Execute the query and store results in a Pandas DataFrame
lineitem_df = pd.read_sql(lineitem_query, mysql_conn)

# Close the MySQL connection
mysql_conn.close()

# Redis connection setup
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch supplier data and load into a Pandas DataFrame
supplier_str = redis_conn.get('supplier').decode('utf-8')
supplier_data = [row.split(',') for row in supplier_str.strip().split('\n')]
supplier_df = pd.DataFrame(supplier_data[1:], columns=supplier_data[0])

# Process the data to find suppliers with maximum revenue
revenue_max = lineitem_df['TOTAL_REVENUE'].max()
max_revenue_df = lineitem_df[lineitem_df['TOTAL_REVENUE'] == revenue_max]
result_df = pd.merge(supplier_df, max_revenue_df, left_on='S_SUPPKEY', right_on='SUPPLIER_NO')
result_df = result_df[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE']]
result_df.sort_values(by='S_SUPPKEY', inplace=True)

# Output the result to a CSV file
result_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_MINIMAL)
