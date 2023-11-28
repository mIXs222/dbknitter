import pymysql
import pandas as pd
from direct_redis import DirectRedis

# MySQL Connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Redis Connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# MySQL Query
mysql_query = """
SELECT n1.N_NAME AS SUPPLIER_NATION, n2.N_NAME AS CUSTOMER_NATION, YEAR(l.SHIPDATE) AS YEAR, 
SUM(l.EXTENDEDPRICE * (1 - l.DISCOUNT)) AS REVENUE
FROM supplier s
JOIN nation n1 ON s.S_NATIONKEY=n1.N_NATIONKEY
JOIN lineitem l ON s.S_SUPPKEY=l.S_SUPPKEY
JOIN orders o ON l.L_ORDERKEY=o.O_ORDERKEY
JOIN customer c ON o.O_CUSTKEY=c.C_CUSTKEY
JOIN nation n2 ON c.C_NATIONKEY=n2.N_NATIONKEY
WHERE ((n1.N_NAME='JAPAN' AND n2.N_NAME='INDIA') OR (n1.N_NAME='INDIA' AND n2.N_NAME='JAPAN'))
  AND l.SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'
GROUP BY SUPPLIER_NATION, CUSTOMER_NATION, YEAR
ORDER BY SUPPLIER_NATION, CUSTOMER_NATION, YEAR
"""

# Execute MySQL query
mysql_df = pd.read_sql(mysql_query, mysql_conn)

# Close the MySQL connection
mysql_conn.close()

# Get Redis dataframe
customer_df = pd.DataFrame(eval(redis_conn.get('customer')))
orders_df = pd.DataFrame(eval(redis_conn.get('orders')))

# Merge Redis dataframes on C_CUSTKEY
merged_df = pd.merge(customer_df, orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# Filter for nation names and years
filtered_df = merged_df[
    ((merged_df['C_NATIONKEY'] == 'JAPAN') & (merged_df['O_ORDERSTATUS'] == 'INDIA')) |
    ((merged_df['C_NATIONKEY'] == 'INDIA') & (merged_df['O_ORDERSTATUS'] == 'JAPAN'))
]
filtered_df = filtered_df[(filtered_df['O_ORDERDATE'] >= '1995-01-01') & (filtered_df['O_ORDERDATE'] <= '1996-12-31')]

# Group by and sum revenue
filtered_df['YEAR'] = pd.to_datetime(filtered_df['O_ORDERDATE']).dt.year
revenue_df = filtered_df.groupby(['C_NATIONKEY', 'O_ORDERSTATUS', 'YEAR']).sum('O_TOTALPRICE').reset_index()

# Rename columns
revenue_df.columns = ['SUPPLIER_NATION', 'CUSTOMER_NATION', 'YEAR', 'REVENUE']

# Combine both result DataFrames
final_df = pd.concat([mysql_df, revenue_df], ignore_index=True)

# Write to CSV
final_df.to_csv('query_output.csv', index=False)
