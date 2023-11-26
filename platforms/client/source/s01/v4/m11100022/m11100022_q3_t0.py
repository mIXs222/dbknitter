# query.py
import pymysql
import pandas as pd
from direct_redis import DirectRedis

# MySql connection and query
mysql_conn = pymysql.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    database="tpch"
)

# MySQL query excluding orders and lineitem
mysql_query = """
    SELECT
        C_CUSTKEY,
        C_MKTSEGMENT
    FROM
        customer
    WHERE
        C_MKTSEGMENT = 'BUILDING'
"""

mysql_df = pd.read_sql(mysql_query, con=mysql_conn)
mysql_conn.close()

# Redis connection and retrieval of tables
redis_conn = DirectRedis(host="redis", port=6379, db=0)
orders_df = pd.read_json(redis_conn.get('orders').decode('utf-8'))
lineitem_df = pd.read_json(redis_conn.get('lineitem').decode('utf-8'))

# Filtering out order dates
orders_df = orders_df[orders_df['O_ORDERDATE'] < '1995-03-15']
lineitem_df = lineitem_df[lineitem_df['L_SHIPDATE'] > '1995-03-15']

# Merge the DataFrames
merged_df = mysql_df.merge(orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = merged_df.merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Execute the core logic of the SQL query on the merged DataFrame
result_df = merged_df.groupby(['L_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY']).apply(
    lambda x: pd.Series({
        'REVENUE': (x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT'])).sum()
    })
).reset_index()

# Sort the result
result_df = result_df.sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True])

# Write result to CSV
result_df.to_csv('query_output.csv', index=False)
