import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connection to MySQL
mysql_conn = pymysql.connect(
    host='mysql', user='root', password='my-secret-pw', database='tpch'
)

# Query to get the data from MySQL tables customer, orders, and lineitem
mysql_query = """
SELECT 
  YEAR(o_orderdate) AS o_year, 
  SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
  orders
JOIN 
  lineitem ON o_orderkey = l_orderkey
JOIN 
  customer ON o_custkey = c_custkey
WHERE 
  o_orderdate BETWEEN '1995-01-01' AND '1996-12-31'
GROUP BY 
  o_year;
"""

# Execute MySQL query
mysql_df = pd.read_sql(mysql_query, mysql_conn)

# Close MySQL connection
mysql_conn.close()

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get tables from Redis as pandas DataFrames
nation_df = pd.read_json(redis_client.get('nation'))
region_df = pd.read_json(redis_client.get('region'))
supplier_df = pd.read_json(redis_client.get('supplier'))
part_df = pd.read_json(redis_client.get('part'))

# Filter and combine Redis tables
asia_nations = region_df[region_df['R_NAME'] == 'ASIA']['R_REGIONKEY']
india_nationkey = nation_df[(nation_df['N_NAME'] == 'INDIA') & (nation_df['N_REGIONKEY'].isin(asia_nations))]['N_NATIONKEY'].iloc[0]
indian_suppliers = supplier_df[supplier_df['S_NATIONKEY'] == india_nationkey]
filtered_parts = part_df[part_df['P_TYPE'] == 'SMALL PLATED COPPER']

# Combine MySQL and Redis data
filtered_lineitems = mysql_df.merge(indian_suppliers, left_on='o_custkey', right_on='S_SUPPKEY')
filtered_lineitems = filtered_lineitems.merge(filtered_parts, left_on='l_partkey', right_on='P_PARTKEY')

# Calculate market share
filtered_lineitems['market_share'] = filtered_lineitems['revenue'] / filtered_lineitems['revenue'].sum()

# Prepare the final DataFrame
market_share_df = filtered_lineitems.groupby('o_year')['market_share'].sum().reset_index()

# Save to CSV
market_share_df.to_csv('query_output.csv', index=False)
