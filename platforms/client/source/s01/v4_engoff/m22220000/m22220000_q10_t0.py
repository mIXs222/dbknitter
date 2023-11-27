import pymysql
import pandas as pd
import csv
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Connect to Redis
redis_conn = DirectRedis(
    host='redis',
    port=6379,
    db=0,
)

try:
    # Query MySQL Tables
    with mysql_conn.cursor() as cursor:
        cursor.execute("""
            SELECT 
                c.C_NAME, 
                c.C_ADDRESS, 
                c.C_CUSTKEY,
                c.C_PHONE, 
                c.C_ACCTBAL, 
                c.C_COMMENT, 
                SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS revenue_lost
            FROM customer c
            JOIN orders o ON c.C_CUSTKEY = o.O_CUSTKEY
            JOIN lineitem l ON l.L_ORDERKEY = o.O_ORDERKEY
            WHERE l.L_RETURNFLAG = 'R'
                AND o.O_ORDERDATE >= '1993-10-01'
                AND o.O_ORDERDATE < '1994-01-01'
            GROUP BY c.C_CUSTKEY
            ORDER BY revenue_lost DESC, c.C_CUSTKEY, c.C_NAME, c.C_ACCTBAL
        """)
        mysql_data = cursor.fetchall()

    # Get Nation Data from Redis
    nation_data = pd.read_json(redis_conn.get('nation').decode('utf-8'))

    # Create DataFrame from MySQL data
    df_mysql_data = pd.DataFrame(
        mysql_data,
        columns=['C_NAME', 'C_ADDRESS', 'C_CUSTKEY', 'C_PHONE', 'C_ACCTBAL', 'C_COMMENT', 'revenue_lost']
    )

    # Merge DataFrames
    df_merged = pd.merge(
        df_mysql_data,
        nation_data,
        how='left',
        left_on='C_NATIONKEY',
        right_on='N_NATIONKEY'
    )

    # Select required columns and write to CSV
    df_result = df_merged[['C_NAME', 'C_ADDRESS', 'N_NAME', 'C_PHONE', 'C_ACCTBAL', 'C_COMMENT', 'revenue_lost']]
    df_result.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_ALL)

finally:
    # Close Connections
    mysql_conn.close()
    redis_conn.close()
