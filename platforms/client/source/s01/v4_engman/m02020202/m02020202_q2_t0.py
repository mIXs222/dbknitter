import pymysql
import pandas as pd
import direct_redis

# Connection details for MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Connection details for Redis
redis_host = 'redis'
redis_port = 6379
redis_db = direct_redis.DirectRedis(host=redis_host, port=redis_port)

# Query for MySQL data
mysql_query = """
SELECT
    n.N_NAME,
    p.P_MFGR,
    ps.PS_PARTKEY as P_PARTKEY,
    s.S_ACCTBAL,
    s.S_ADDRESS,
    s.S_COMMENT,
    s.S_NAME,
    s.S_PHONE
FROM
    partsupp as ps
JOIN part as p ON ps.PS_PARTKEY = p.P_PARTKEY
JOIN supplier as s ON ps.PS_SUPPKEY = s.S_SUPPKEY
JOIN nation as n ON s.S_NATIONKEY = n.N_NATIONKEY
WHERE
    p.P_TYPE = 'BRASS' and p.P_SIZE = 15
"""

# Execute MySQL query
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    mysql_result = cursor.fetchall()

# Convert MySQL result to DataFrame
mysql_df = pd.DataFrame(list(mysql_result), columns=[
    'N_NAME', 'P_MFGR', 'P_PARTKEY', 'S_ACCTBAL', 'S_ADDRESS', 'S_COMMENT', 'S_NAME', 'S_PHONE'])

# Extract necessary data from Redis
region_df = pd.read_json(redis_db.get('region').decode('utf-8'))
supplier_df = pd.read_json(redis_db.get('supplier').decode('utf-8'))

# Merge Redis and MySQL data
merged_df = mysql_df.merge(supplier_df, how='left', left_on='S_NAME', right_on='S_NAME')
merged_df = merged_df.merge(region_df, how='left', left_on='N_NAME', right_on='R_NAME')

# Filter for the EUROPE region
europe_df = merged_df[merged_df['R_NAME'] == 'EUROPE']

# Select minimum cost suppliers
europe_min_cost_df = europe_df.groupby('P_PARTKEY', as_index=False).apply(lambda x: x.nsmallest(1, 'PS_SUPPLYCOST'))

# Order the suppliers
europe_min_cost_df_sorted = europe_min_cost_df.sort_values(
    by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'],
    ascending=[False, True, True, True]
)

# Select relevant columns
output_df = europe_min_cost_df_sorted[
    ['N_NAME', 'P_MFGR', 'P_PARTKEY', 'S_ACCTBAL', 'S_ADDRESS', 'S_COMMENT', 'S_NAME', 'S_PHONE']]

# Write results to CSV
output_df.to_csv('query_output.csv', index=False)
