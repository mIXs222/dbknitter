import pymysql
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection parameters
mysql_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch',
}

# Redis connection parameters
redis_params = {
    'host': 'redis',
    'port': 6379,
    'db': 0,
}

# Connect to MySQL
mysql_conn = pymysql.connect(**mysql_params)
cursor = mysql_conn.cursor()

# Connect to Redis
redis_conn = DirectRedis(**redis_params)

# Load DataFrames from Redis
nation_df = pd.read_json(redis_conn.get('nation'))
region_df = pd.read_json(redis_conn.get('region'))
supplier_df = pd.read_json(redis_conn.get('supplier'))

# Join nation and region tables
nation_region_df = nation_df.merge(region_df, left_on='N_REGIONKEY', right_on='R_REGIONKEY')

# Filter for suppliers from 'INDIA' in 'ASIA'
asia_nations = nation_region_df[nation_region_df['R_NAME'] == 'ASIA']
india_suppliers = supplier_df[supplier_df['S_NATIONKEY'].isin(asia_nations[asia_nations['N_NAME'] == 'INDA']['N_NATIONKEY'])]

# Query for lineitems in the years 1995 and 1996
query = """
SELECT L_PARTKEY, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS revenue, YEAR(O_ORDERDATE) AS year
FROM lineitem
JOIN orders ON L_ORDERKEY = O_ORDERKEY
WHERE L_PARTKEY IN (SELECT P_PARTKEY FROM part WHERE P_TYPE = 'SMALL PLATED COPPER') 
AND YEAR(O_ORDERDATE) IN (1995, 1996)
AND O_CUSTKEY IN (SELECT C_CUSTKEY FROM customer WHERE C_NATIONKEY = %s)
GROUP BY L_PARTKEY, year
ORDER BY year;
"""
india_suppliers_keys = tuple(india_suppliers['S_SUPPKEY'].unique())
cursor.execute(query, india_suppliers_keys)
results = cursor.fetchall()

# Create a DataFrame to store the query results
df_results = pd.DataFrame(results, columns=['partkey', 'revenue', 'year'])

# Close connections
cursor.close()
mysql_conn.close()

# Write the output to a CSV file
df_results.to_csv('query_output.csv', index=False)
