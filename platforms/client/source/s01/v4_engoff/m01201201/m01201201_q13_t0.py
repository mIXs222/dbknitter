import pandas as pd
import pymysql
from direct_redis import DirectRedis
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Execute MySQL query and close the connection
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT O_CUSTKEY, COUNT(*) AS ORDER_COUNT FROM orders WHERE O_ORDERSTATUS NOT IN ('PENDING', 'DEPOSIT') GROUP BY O_CUSTKEY")
    orders_result = cursor.fetchall()
mysql_conn.close()

# Connect to Redis
redis = DirectRedis(host='redis', port=6379, db=0)

# Get the customer table from Redis
customer_data = eval(redis.get('customer'))

# Create DataFrame from the Redis data
customer_df = pd.DataFrame(customer_data, columns=['C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 
                                                   'C_NATIONKEY', 'C_PHONE', 'C_ACCTBAL', 
                                                   'C_MKTSEGMENT', 'C_COMMENT'])

# Create DataFrame from the MySQL data
orders_df = pd.DataFrame(list(orders_result), columns=['O_CUSTKEY', 'ORDER_COUNT'])

# Convert CUSTKEYs to integers to ensure proper join
customer_df['C_CUSTKEY'] = customer_df['C_CUSTKEY'].astype(int)
orders_df['O_CUSTKEY'] = orders_df['O_CUSTKEY'].astype(int)

# Merge customers with orders DataFrame to include customers with no orders
merged_df = pd.merge(customer_df, orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='left')

# Replace NaN with 0 for customers without orders
merged_df['ORDER_COUNT'] = merged_df['ORDER_COUNT'].fillna(0)

# Get the distribution of customers by the number of orders
customer_distribution = merged_df['ORDER_COUNT'].value_counts().sort_index().reset_index()
customer_distribution.columns = ['NUMBER_OF_ORDERS', 'CUSTOMER_COUNT']

# Write query's output to a csv file
customer_distribution.to_csv('query_output.csv', index=False)
