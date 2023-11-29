import pymysql
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Redis connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Define the query for MySQL
mysql_query = """
SELECT
  s.S_SUPPKEY,
  s.S_NAME,
  SUM(l.L_QUANTITY) AS total_quantity
FROM
  supplier s
JOIN
  lineitem l ON s.S_SUPPKEY = l.L_SUPPKEY
WHERE
  l.L_SHIPDATE >= '1994-01-01' AND
  l.L_SHIPDATE < '1995-01-01'
GROUP BY
  s.S_SUPPKEY
HAVING
  total_quantity > (
    SELECT 
      0.5 * SUM(l2.L_QUANTITY)
    FROM
      lineitem l2
    JOIN
      part p ON l2.L_PARTKEY = p.P_PARTKEY
    WHERE
      p.P_NAME LIKE '%forest%' AND
      l2.L_SHIPDATE >= '1994-01-01' AND
      l2.L_SHIPDATE < '1995-01-01'
  )
"""

# Execute MySQL query
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    suppliers_data = cursor.fetchall()

# Convert to DataFrame
supplier_df = pd.DataFrame(suppliers_data, columns=['S_SUPPKEY', 'S_NAME', 'total_quantity'])

# Get the nation table from Redis
nation_df = pd.read_json(redis_conn.get('nation').decode('utf-8'))

# Filter for Canada nation key
canada_nation_key = nation_df[nation_df['N_NAME'] == 'CANADA']['N_NATIONKEY'].iloc[0]

# Get the partsupp table from Redis
partsupp_df = pd.read_json(redis_conn.get('partsupp').decode('utf-8'))

# Merge the data frames
final_df = supplier_df.merge(partsupp_df, how='inner', left_on='S_SUPPKEY', right_on='PS_SUPPKEY')

# Filter for Canada
final_df = final_df[final_df['PS_AVAILQTY'] > (0.5 * final_df['total_quantity'])]
final_df = final_df[final_df['N_NATIONKEY'] == canada_nation_key]

# Write the result to a CSV file
final_df.to_csv('query_output.csv', index=False)

# Close the connections
mysql_conn.close()
redis_conn.close()
