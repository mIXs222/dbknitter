import pandas as pd
import pymysql
import direct_redis
from collections import Counter

# Function to connect to MySQL and get the customer data.
def get_mysql_customer_data():
    connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT C_CUSTKEY, C_NAME FROM customer")
            result = cursor.fetchall()
            customer_df = pd.DataFrame(list(result), columns=['C_CUSTKEY', 'C_NAME'])
        return customer_df
    finally:
        connection.close()

# Function to connect to Redis and get the orders data as a DataFrame.
def get_redis_orders_data():
    redis_client = direct_redis.DirectRedis(host="redis", port=6379, db=0)
    orders_data = redis_client.get('orders')
    if orders_data is not None:
        orders_df = pd.read_json(orders_data)
        return orders_df
    else:
        return pd.DataFrame(columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_COMMENT'])

# Get the customer data from MySQL.
customer_df = get_mysql_customer_data()

# Get the orders data from Redis.
orders_df = get_redis_orders_data()

# Filter out orders with status 'pending' or 'deposits'.
orders_df = orders_df[~(orders_df['O_ORDERSTATUS'].str.contains('pending') |
                        orders_df['O_COMMENT'].str.contains('deposits'))]

# Count the number of orders per customer excluding the special categories.
orders_count = Counter(orders_df['O_CUSTKEY'])

# Create a DataFrame to count the distribution of order counts.
customer_order_counts = pd.DataFrame.from_dict(orders_count, orient='index').reset_index()
customer_order_counts.columns = ['C_CUSTKEY', 'ORDER_COUNT']

# Merge the data to include all customers and their respective orders count.
final_df = pd.merge(customer_df, customer_order_counts, on='C_CUSTKEY', how='left')

# Replace NaN with 0 for customers with no orders and make sure ORDER_COUNT is integer.
final_df['ORDER_COUNT'] = final_df['ORDER_COUNT'].fillna(0).astype(int)

# Get distribution of customers by number of orders.
distribution = final_df['ORDER_COUNT'].value_counts().sort_index().reset_index()
distribution.columns = ['NUMBER_OF_ORDERS', 'CUSTOMER_COUNT']

# Write the query output to a CSV file.
distribution.to_csv('query_output.csv', index=False)
