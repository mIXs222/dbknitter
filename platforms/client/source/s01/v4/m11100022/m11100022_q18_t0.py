# query.py
import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', passwd='my-secret-pw', db='tpch')

# Run query for customer table in MySQL
mysql_query = """
    SELECT
        C_NAME,
        C_CUSTKEY
    FROM
        customer
"""
with mysql_conn:
    customer_df = pd.read_sql(mysql_query, mysql_conn)

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Retrieve orders and lineitem table from Redis
orders_df = pd.read_json(redis_conn.get('orders'))
lineitem_df = pd.read_json(redis_conn.get('lineitem'))

# Aggregate lineitem to filter on sum of L_QUANTITY > 300
lineitem_agg = lineitem_df.groupby('L_ORDERKEY').filter(lambda x: x['L_QUANTITY'].sum() > 300)

# Merge dataframes to simulate joins
merged_df = (
    lineitem_agg[['L_ORDERKEY']]
    .merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY', how='inner')
    .merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY', how='inner')
    .merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY', how='inner')
)

# Select the required columns and perform group by
result_df = (
    merged_df.groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE'])
    .agg({'L_QUANTITY': 'sum', 'O_TOTALPRICE': 'first', 'O_ORDERDATE': 'first'})
    .reset_index()
)

# Sort the results as per the original query
result_df = result_df.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])

# Write the result to CSV
result_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
redis_conn.close()
