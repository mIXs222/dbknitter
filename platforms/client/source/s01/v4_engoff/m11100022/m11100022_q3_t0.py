# Import necessary libraries
import pymysql
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime
import csv

# Establish connection to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Query to retrieve customer data with market segment as BUILDING
mysql_query = """
SELECT C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_COMMENT
FROM customer
WHERE C_MKTSEGMENT = 'BUILDING';
"""

# Execute the MySQL query
mysql_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# Establish connection to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Retrieve orders and lineitem dataframes from Redis
orders_df = pd.read_json(redis_conn.get('orders'))
lineitem_df = pd.read_json(redis_conn.get('lineitem'))

# Filter the datasets with given conditions and compute revenue
lineitem_df['revenue'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])
filtered_lineitem_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] > '1995-03-15')
]

max_order_revenue_df = filtered_lineitem_df.groupby('L_ORDERKEY')['revenue'].sum().reset_index()
max_order_revenue_df = max_order_revenue_df.rename(columns={'revenue': 'potential_revenue'})
max_order_revenue = max_order_revenue_df['potential_revenue'].max()
filtered_orders_df = orders_df[
    (orders_df['O_ORDERKEY'].isin(max_order_revenue_df['L_ORDERKEY'])) & 
    (orders_df['O_ORDERSTATUS'] == 'O')
]

# Join orders with max revenue with customer data on customer key
final_result_df = pd.merge(
    filtered_orders_df[['O_ORDERKEY', 'O_CUSTKEY', 'O_SHIPPRIORITY']],
    mysql_df,
    left_on='O_CUSTKEY', right_on='C_CUSTKEY'
)

# Merge with max revenue data and sort by potential revenue in descending order
final_result_df = pd.merge(final_result_df, max_order_revenue_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
final_result_df = final_result_df[final_result_df['potential_revenue'] == max_order_revenue]
final_result_df = final_result_df.sort_values('potential_revenue', ascending=False)

# Write final result to CSV
final_result_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
