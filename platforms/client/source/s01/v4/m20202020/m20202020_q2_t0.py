import csv
import pymysql
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.Cursor
)

# Redis connection using direct_redis.DirectRedis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Execute MySQL query
with mysql_conn.cursor() as cursor:
    mysql_query = """
    SELECT
        S.S_ACCTBAL,
        S.S_NAME,
        N_NAME,
        S.S_ADDRESS,
        S.S_PHONE,
        S.S_COMMENT
    FROM
        supplier S
    INNER JOIN
        nation N ON S.S_NATIONKEY = N.N_NATIONKEY
    INNER JOIN
        region ON N.N_REGIONKEY = R_REGIONKEY
    WHERE
        R.R_NAME = 'EUROPE'
    """
    cursor.execute(mysql_query)
    mysql_results = cursor.fetchall()
mysql_conn.close()

# Read Redis data into pandas DataFrames
df_nation = pd.read_msgpack(redis_conn.get('nation'))
df_part = pd.read_msgpack(redis_conn.get('part'))
df_partsupp = pd.read_msgpack(redis_conn.get('partsupp'))

# Filtering parts and partsupp for the conditions
df_parts_filtered = df_part[(df_part['P_SIZE'] == 15) & (df_part['P_TYPE'].str.contains('BRASS'))]
df_partsupp_filtered = df_partsupp[df_partsupp['PS_PARTKEY'].isin(df_parts_filtered['P_PARTKEY'])]

# Finding the minimum PS_SUPPLYCOST for EUROPE
min_supply_cost = df_partsupp_filtered[df_partsupp_filtered['PS_SUPPLYCOST'] == df_partsupp_filtered['PS_SUPPLYCOST'].min()]

# Merge results based on the PS_SUPPLYCOST
merged_df = pd.merge(
    min_supply_cost,
    df_parts_filtered,
    left_on='PS_PARTKEY',
    right_on='P_PARTKEY',
    how='inner'
)

# Convert MySQL results to DataFrame and merge with redis dataframes
mysql_df = pd.DataFrame(mysql_results, columns=['S_ACCTBAL', 'S_NAME', 'N_NAME', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT'])
combined_df = pd.merge(
    mysql_df,
    merged_df,
    left_on=['S_NAME', 'N_NAME'],
    right_on=['PS_SUPPKEY', 'PS_SUPPLYCOST'],
    how='inner'
)

# Final filtering of the columns as per the SELECT clause
final_df = combined_df[['S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT']]

# Sort the final DataFrame
final_df_sorted = final_df.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

# Write final results to CSV
final_df_sorted.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_MINIMAL)
