import pymysql
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Execute query part for MySQL tables
with mysql_conn.cursor() as cursor:
    cursor.execute("""
    SELECT
        n.N_NATIONKEY,
        o.O_ORDERDATE,
        p.P_TYPE,
        (l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) - (ps.PS_SUPPLYCOST * l.L_QUANTITY) AS profit
    FROM
        orders o
    JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
    JOIN partsupp ps ON l.L_PARTKEY = ps.PS_PARTKEY AND l.L_SUPPKEY = ps.PS_SUPPKEY
    JOIN part p ON l.L_PARTKEY = p.P_PARTKEY
    JOIN nation n ON ps.PS_SUPPKEY = n.N_NATIONKEY
    WHERE
        p.P_TYPE LIKE '%[SPECIFIED DIM]%'
    """)
    mysql_data = cursor.fetchall()

# Close the MySQL connection
mysql_conn.close()

# Convert MySQL data to DataFrame
mysql_df = pd.DataFrame(mysql_data, columns=["nationkey", "orderdate", "ptype", "profit"])

# Connect to Redis
r = DirectRedis(host='redis', port=6379, db=0)

# Get 'supplier' table from Redis
supplier_df = pd.read_json(r.get('supplier'))

# Merge MySQL and Redis data based on supplier nation keys
result = pd.merge(mysql_df, supplier_df, left_on='nationkey', right_on='S_NATIONKEY')

# Extract year from orderdate and perform grouping and aggregation for profit calculation
result['year'] = pd.to_datetime(result['orderdate']).dt.year
final_df = result.groupby(['S_NAME', 'year'])['profit'].sum().reset_index()

# Sort the dataframe
final_df = final_df.sort_values(['S_NAME', 'year'], ascending=[True, False])

# Write output to CSV
final_df.to_csv('query_output.csv', index=False)
