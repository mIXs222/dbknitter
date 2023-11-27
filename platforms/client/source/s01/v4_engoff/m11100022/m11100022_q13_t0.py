import pymysql
import pandas as pd
import direct_redis

# MySQL connection
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   database='tpch')

# Query to get customers from MySQL
customer_query = "SELECT * FROM customer;"
customer_df = pd.read_sql(customer_query, mysql_connection)
mysql_connection.close()

# Connect to Redis
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)
# Get orders from Redis
orders_df = pd.read_json(redis_connection.get('orders'))

# Clean orders Data Frame (filter out pending or deposits)
orders_df = orders_df[~orders_df['O_COMMENT'].str.contains('pending|deposits', case=False, na=False)]

# Aggregate the number of orders each customer has
orders_grouped = orders_df.groupby('O_CUSTKEY').size().reset_index(name='order_count')

# Merge with the customers DataFrame
customer_orders_merged = pd.merge(customer_df, orders_grouped, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='left')

# Replace NaN values in order_count with 0 (those customers have no orders)
customer_orders_merged['order_count'] = customer_orders_merged['order_count'].fillna(0)

# Count the number of customers by number of orders
customers_by_order_count = customer_orders_merged.groupby('order_count').size().reset_index(name='number_of_customers')

# Write the output to a CSV file
customers_by_order_count.to_csv('query_output.csv', index=False)
