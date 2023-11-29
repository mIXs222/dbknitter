import pymysql
import direct_redis
import pandas as pd

# MySQL connection and query
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_query = """
SELECT c.C_CUSTKEY, sum(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) as REVENUE
FROM customer c
JOIN orders o ON c.C_CUSTKEY = o.O_CUSTKEY
JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
WHERE o.O_ORDERDATE BETWEEN '1990-01-01' AND '1995-01-01'
GROUP BY c.C_CUSTKEY;
"""

mysql_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# Redis connection and data retrieval
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)
nation_df = pd.DataFrame(redis_conn.get('nation'))
region_df = pd.DataFrame(redis_conn.get('region'))
supplier_df = pd.DataFrame(redis_conn.get('supplier'))

# Merge and filter data
asia_region = region_df[region_df['R_NAME'] == 'ASIA']['R_REGIONKEY'].iloc[0]
asia_nations = nation_df[nation_df['N_REGIONKEY'] == asia_region]

# Identify customers and suppliers in Asia
asia_customers = mysql_df[mysql_df['C_NATIONKEY'].isin(asia_nations['N_NATIONKEY'])]
asia_suppliers = supplier_df[supplier_df['S_NATIONKEY'].isin(asia_nations['N_NATIONKEY'])]

# Merge to get the final DataFrame
final_df = pd.merge(asia_customers, asia_suppliers, left_on='C_NATIONKEY', right_on='S_NATIONKEY')
final_df = final_df.groupby(['N_NAME'])['REVENUE'].sum().reset_index()
final_df.sort_values(by='REVENUE', inplace=True, ascending=False)

# Write the result to a CSV file
final_df.to_csv('query_output.csv', index=False)
