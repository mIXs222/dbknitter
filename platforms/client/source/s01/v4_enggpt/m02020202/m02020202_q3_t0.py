import pymysql
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection setup
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Redis connection setup
redis_conn = DirectRedis(host='redis', port=6379)

# Retrieve MySQL data for 'orders' table
query_orders = """
SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERDATE, O_SHIPPRIORITY 
FROM orders 
WHERE O_ORDERDATE < '1995-03-15';
"""
orders_df = pd.read_sql(query_orders, mysql_conn)

# Retrieve Redis data for 'customer' and 'lineitem' tables
customer_df = pd.read_json(redis_conn.get('customer').decode('utf-8'), orient='records')
lineitem_df = pd.read_json(redis_conn.get('lineitem').decode('utf-8'), orient='records')

# Filter 'customer' and 'lineitem' data based on given criteria
customer_df = customer_df[customer_df['C_MKTSEGMENT'] == 'BUILDING']
lineitem_df = lineitem_df[lineitem_df['L_SHIPDATE'] > '1995-03-15']

# Merge dataframes on 'C_CUSTKEY' and 'O_CUSTKEY', then 'L_ORDERKEY' and 'O_ORDERKEY'
merged_df = pd.merge(customer_df, orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = pd.merge(merged_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Compute the revenue
merged_df['revenue'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Group by 'O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY', and sum the revenue
grouped_results = merged_df.groupby(['O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'])['revenue'].sum().reset_index()

# Sort the results by revenue desc, and order date asc
sorted_results = grouped_results.sort_values(by=['revenue', 'O_ORDERDATE'], ascending=[False, True])

# Write to CSV file
sorted_results.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
redis_conn.close()
