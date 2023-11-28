import pymysql
import pandas as pd
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Read 'customer' table from MySQL
mysql_query = """
SELECT C_CUSTKEY, C_MKTSEGMENT
FROM customer
WHERE C_MKTSEGMENT = 'BUILDING';
"""
customer_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Read 'orders' table from Redis
orders_df = pd.DataFrame(redis_conn.get('orders'))  # Assuming get() returns data in a format compatible with DataFrame constructor

# Read 'lineitem' table from Redis
lineitem_df = pd.DataFrame(redis_conn.get('lineitem'))  # Assuming get() returns data in a format compatible with DataFrame constructor

# Filter orders before March 15, 1995
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
orders_filtered_df = orders_df[orders_df['O_ORDERDATE'] < '1995-03-15']

# Filter lineitem shipped after March 15, 1995
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
lineitem_filtered_df = lineitem_df[lineitem_df['L_SHIPDATE'] > '1995-03-15']

# Calculate revenue
lineitem_filtered_df['REVENUE'] = lineitem_filtered_df['L_EXTENDEDPRICE'] * (1 - lineitem_filtered_df['L_DISCOUNT'])

# Merge dataframes
merged_df = customer_df \
    .merge(orders_filtered_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY') \
    .merge(lineitem_filtered_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Group by order key, order date, and ship priority
grouped_df = merged_df.groupby(['O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'])

# Sum revenue and reset index
result_df = grouped_df['REVENUE'].sum().reset_index()

# Order by revenue desc, order date asc
final_df = result_df.sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True])

# Write to CSV
final_df.to_csv('query_output.csv', index=False)
