import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to Redis
redis = DirectRedis(host='redis', port=6379, db=0)

# Query MySQL to get customer and lineitem data
mysql_query = """
SELECT
    c.C_CUSTKEY,
    c.C_NAME,
    l.L_EXTENDEDPRICE,
    l.L_DISCOUNT,
    c.C_ACCTBAL,
    c.C_ADDRESS,
    c.C_PHONE,
    c.C_COMMENT,
    l.L_ORDERKEY
FROM
    customer c
JOIN
    lineitem l
ON
    c.C_CUSTKEY = l.L_ORDERKEY
WHERE
    l.L_RETURNFLAG = 'R'
"""
mysql_cursor.execute(mysql_query)
mysql_result = mysql_cursor.fetchall()

# Convert MySQL data to DataFrame
mysql_df = pd.DataFrame(
    mysql_result,
    columns=['C_CUSTKEY', 'C_NAME', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'C_ACCTBAL',
             'C_ADDRESS', 'C_PHONE', 'C_COMMENT', 'L_ORDERKEY']
)

# Query Redis to get orders and nation data
orders_df = pd.DataFrame(eval(redis.get('orders')))
nation_df = pd.DataFrame(eval(redis.get('nation')))

# Filter orders by date
orders_df = orders_df[
    (orders_df['O_ORDERDATE'] >= '1993-10-01') &
    (orders_df['O_ORDERDATE'] < '1994-01-01')
]

# Merge DataFrames
merged_df = mysql_df.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merged_df = merged_df.merge(nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Calculate revenue
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Group data
grouped_df = merged_df.groupby(
    ['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_PHONE', 'N_NAME', 'C_ADDRESS', 'C_COMMENT'],
    as_index=False
)['REVENUE'].sum()

# Sort the grouped data
sorted_df = grouped_df.sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[False, True, True, False])

# Write to CSV
sorted_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_cursor.close()
mysql_conn.close()
