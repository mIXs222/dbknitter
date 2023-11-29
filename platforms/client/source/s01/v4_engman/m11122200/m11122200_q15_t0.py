import pymysql
import pandas as pd
import direct_redis

# MySQL connection and query
mysql_conn = pymysql.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    database="tpch"
)

query_mysql = """
SELECT L_SUPPKEY, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS TOTAL_REVENUE
FROM lineitem
WHERE L_SHIPDATE >= '1996-01-01' AND L_SHIPDATE < '1996-04-01'
GROUP BY L_SUPPKEY
"""

lineitem_rev_df = pd.read_sql(query_mysql, con=mysql_conn)
mysql_conn.close()

# Redis connection and getting the supplier data
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

supplier = redis_conn.get('supplier')
supplier_df = pd.read_json(supplier if supplier else '[]')

# Merging DataFrames and filtering the top supplier(s)
merged_df = pd.merge(
    supplier_df,
    lineitem_rev_df,
    left_on='S_SUPPKEY',
    right_on='L_SUPPKEY'
)

max_revenue = merged_df['TOTAL_REVENUE'].max()
top_suppliers = merged_df[merged_df['TOTAL_REVENUE']==max_revenue]

# Selecting specific columns to output
output_columns = ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE']
top_suppliers = top_suppliers[output_columns].sort_values(by='S_SUPPKEY')

# Writing to CSV file
top_suppliers.to_csv('query_output.csv', index=False)
