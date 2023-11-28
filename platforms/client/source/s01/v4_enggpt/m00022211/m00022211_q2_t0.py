import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
conn_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}
mysql_conn = pymysql.connect(**conn_info)

# Query for MySQL
mysql_query = """
SELECT 
  p.P_PARTKEY, p.P_NAME, p.P_MFGR, p.P_SIZE, n.N_NATIONKEY, n.N_NAME,
  r.R_NAME, p.P_TYPE, ps.PS_SUPPLYCOST, s.S_ACCTBAL, s.S_NAME,
  s.S_ADDRESS, s.S_PHONE, s.S_COMMENT
FROM
  part p
JOIN
  partsupp ps ON p.P_PARTKEY = ps.PS_PARTKEY
JOIN
  supplier s ON ps.PS_SUPPKEY = s.S_SUPPKEY
JOIN
  nation n ON s.S_NATIONKEY = n.N_NATIONKEY
JOIN
  region r ON n.N_REGIONKEY = r.R_REGIONKEY
WHERE
  p.P_SIZE = 15 AND p.P_TYPE LIKE '%BRASS'
  AND r.R_NAME = 'EUROPE';
"""
mysql_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get data from Redis
supplier_df = pd.DataFrame(eval(redis_client.get('supplier')))
partsupp_df = pd.DataFrame(eval(redis_client.get('partsupp')))

# Joining DataFrames to get the redis data we need
redis_df = pd.merge(supplier_df, partsupp_df, how='inner', left_on='S_SUPPKEY', right_on='PS_SUPPKEY')

# Combine MySQL and Redis data
combined_df = mysql_df.merge(redis_df, left_on=['P_PARTKEY', 'S_NATIONKEY'], right_on=['PS_PARTKEY', 'S_NATIONKEY'])

# Ordering
combined_df_sorted = combined_df.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

# Write to CSV
combined_df_sorted.to_csv('query_output.csv', index=False)
