import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Query MySQL for customer data
customer_query = """
SELECT C_CUSTKEY, C_NAME FROM customer
"""
customers_df = pd.read_sql(customer_query, mysql_conn)
mysql_conn.close()

# Connect to Redis database
redis_conn = DirectRedis(host='redis', port=6379, db=0)
orders_df = redis_conn.get('orders')
orders_df = pd.DataFrame(orders_df)

# Filter orders based on the comment criteria
orders_df = orders_df[~orders_df['O_COMMENT'].str.contains('pending|deposits', regex=True)]

# Count the number of orders for each customer
orders_count_df = orders_df['O_CUSTKEY'].value_counts().reset_index()
orders_count_df.columns = ['C_CUSTKEY', 'OrderCount']

# Merge customers data with orders count
merged_df = pd.merge(customers_df, orders_count_df, on='C_CUSTKEY', how='left')
merged_df['OrderCount'] = merged_df['OrderCount'].fillna(0)

# Calculate the distribution of customers by the number of orders
customers_distribution_df = merged_df['OrderCount'].value_counts().reset_index()
customers_distribution_df.columns = ['NumberOfOrders', 'NumberOfCustomers']

# Write the output to a CSV file
customers_distribution_df.to_csv('query_output.csv', index=False)
