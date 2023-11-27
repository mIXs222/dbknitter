import pandas as pd
import pymysql
from pymysql.cursors import Cursor
import direct_redis
import csv

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Redis connection
redis_conn = direct_redis.DirectRedis(
    host='redis',
    port=6379,
    db=0
)

# Execute MySQL query
with mysql_conn.cursor() as cursor:
    cursor.execute("""
    SELECT
        partsupp.PS_PARTKEY,
        supplier.S_SUPPKEY,
        supplier.S_NATIONKEY,
        partsupp.PS_AVAILQTY,
        partsupp.PS_SUPPLYCOST
    FROM
        partsupp
    INNER JOIN
        supplier ON partsupp.PS_SUPPKEY = supplier.S_SUPPKEY
    """)
    
    # Fetch the result
    mysql_result = cursor.fetchall()

# Convert to DataFrame
df_mysql = pd.DataFrame(mysql_result, columns=['PS_PARTKEY', 'S_SUPPKEY', 'S_NATIONKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST'])

# Fetch nation table from Redis
df_nation = pd.read_msgpack(redis_conn.get('nation'))

# Merge MySQL and Redis data
merged_df = pd.merge(
    df_mysql,
    df_nation[df_nation['N_NAME'] == 'GERMANY'],
    how='inner',
    left_on='S_NATIONKEY',
    right_on='N_NATIONKEY'
)

# Perform the aggregation
grouped_df = merged_df.groupby('PS_PARTKEY').agg(VALUE=('PS_SUPPLYCOST', lambda x: (x * merged_df['PS_AVAILQTY']).sum()))

# Filter based on the having clause
sum_ps_supplycost = (merged_df['PS_SUPPLYCOST'] * merged_df['PS_AVAILQTY']).sum() * 0.0001000000
final_df = grouped_df[grouped_df['VALUE'] > sum_ps_supplycost]

# Sort by value
final_df_sorted = final_df.sort_values(by='VALUE', ascending=False)

# Write the result to a CSV file
final_df_sorted.to_csv('query_output.csv', index=False)

# Close the MySQL connection
mysql_conn.close()
