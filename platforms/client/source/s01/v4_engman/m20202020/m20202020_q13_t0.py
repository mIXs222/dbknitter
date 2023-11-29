import pandas as pd
import pymysql
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to Redis
redis_conn = DirectRedis(
    host='redis',
    port=6379,
    db=0
)

# Fetch customers from MySQL
customer_query = "SELECT C_CUSTKEY FROM customer"
customer_df = pd.read_sql(customer_query, mysql_conn)

# Fetch orders from Redis
orders_df = pd.read_json(redis_conn.get('orders'), orient='records')

# Merge customer and orders DataFrame on C_CUSTKEY
merged_df = pd.merge(customer_df, orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='inner')

# Filter orders that are not pending and do not have deposits
valid_orders_df = merged_df[~merged_df['O_COMMENT'].str.contains('pending|deposits', regex=True)]

# Aggregate to find the number of orders per customer
orders_per_customer = valid_orders_df.groupby('C_CUSTKEY').size().reset_index(name='num_orders')

# Count the number of customers per number of orders and rename columns for clarity
distribution_of_customers = (
    orders_per_customer.groupby('num_orders')
    .size()
    .reset_index(name='num_customers')
)

# Write results to CSV
distribution_of_customers.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
redis_conn.close()
