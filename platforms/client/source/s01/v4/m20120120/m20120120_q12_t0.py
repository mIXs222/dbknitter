# Filename: query_execute.py

import pymysql
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection details
mysql_connection_details = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}

# Redis connection details
redis_connection_details = {
    'host': 'redis',
    'port': 6379,
    'db': 0
}

# Connect to Redis and retrieve orders DataFrame
redis_client = DirectRedis(**redis_connection_details)
orders_df = pd.read_json(redis_client.get('orders'))

# Filter orders with URGENT or HIGH priority
orders_high_priority_condition = (
    (orders_df["O_ORDERPRIORITY"] == "1-URGENT") |
    (orders_df["O_ORDERPRIORITY"] == "2-HIGH")
)
orders_df["HIGH_LINE_COUNT"] = orders_high_priority_condition.astype(int)
orders_df["LOW_LINE_COUNT"] = (~orders_high_priority_condition).astype(int)

# Connect to MySQL and retrieve lineitem DataFrame
connection = pymysql.connect(**mysql_connection_details)
query = """
SELECT L_ORDERKEY, L_SHIPMODE
FROM lineitem
WHERE
    L_SHIPMODE IN ('MAIL', 'SHIP')
    AND L_COMMITDATE < L_RECEIPTDATE
    AND L_SHIPDATE < L_COMMITDATE
    AND L_RECEIPTDATE >= '1994-01-01'
    AND L_RECEIPTDATE < '1995-01-01'
"""
lineitem_df = pd.read_sql(query, connection)
connection.close()

# Merge dataframes on O_ORDERKEY and L_ORDERKEY
merged_df = orders_df.merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Perform the group by
result_df = merged_df.groupby('L_SHIPMODE').agg({
    'HIGH_LINE_COUNT': 'sum',
    'LOW_LINE_COUNT': 'sum'
}).reset_index()

# Write the result to query_output.csv
result_df.to_csv('query_output.csv', index=False)
