import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Fetch data from MySQL
query_mysql = """
SELECT c.C_CUSTKEY, o.O_ORDERKEY, o.O_ORDERDATE, l.L_EXTENDEDPRICE, l.L_DISCOUNT
FROM customer c
JOIN orders o ON c.C_CUSTKEY = o.O_CUSTKEY
JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
WHERE o.O_ORDERDATE >= '1990-01-01' AND o.O_ORDERDATE <= '1994-12-31'
"""
df_mysql = pd.read_sql(query_mysql, mysql_conn)

# Close the MySQL connection
mysql_conn.close()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379)

# Fetch data from Redis
df_nation = pd.DataFrame(eval(redis_conn.get('nation')))
df_region = pd.DataFrame(eval(redis_conn.get('region')))

# Filter for 'ASIA' region
asia_region = df_region[df_region['R_NAME'] == 'ASIA']
asia_nations = df_nation[df_nation['N_REGIONKEY'].isin(asia_region['R_REGIONKEY'])]

# Merge the data
df_merged = pd.merge(
    df_mysql,
    asia_nations[['N_NATIONKEY', 'N_NAME']],
    left_on='C_NATIONKEY',
    right_on='N_NATIONKEY',
    how='inner'
)

# Calculate the revenue
df_merged['REVENUE'] = df_merged['L_EXTENDEDPRICE'] * (1 - df_merged['L_DISCOUNT'])

# Sum revenue by nation
revenue_by_nation = df_merged.groupby('N_NAME')['REVENUE'].sum().reset_index()

# Sort the results
revenue_by_nation_sorted = revenue_by_nation.sort_values(by='REVENUE', ascending=False)

# Output to CSV
revenue_by_nation_sorted.to_csv('query_output.csv', index=False)
