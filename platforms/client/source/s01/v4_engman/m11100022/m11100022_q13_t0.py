import pymysql
import pandas as pd
import csv
from direct_redis import DirectRedis

# Function to get data from MySQL
def get_mysql_data():
    connection = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 db='tpch')
    try:
        with connection.cursor() as cursor:
            sql = "SELECT C_CUSTKEY FROM customer;"
            cursor.execute(sql)
            result = cursor.fetchall()
            customer_data = pd.DataFrame(list(result), columns=['C_CUSTKEY'])
    finally:
        connection.close()
    return customer_data

# Function to get data from Redis
def get_redis_data():
    r = DirectRedis(host='redis', port=6379, db=0)
    orders_data = pd.read_json(r.get('orders'), orient='records')
    return orders_data

# Get data from MySQL and Redis
customer_data = get_mysql_data()
orders_data = get_redis_data()

# Merge and process data
merged_data = pd.merge(customer_data, orders_data, how='left', left_on='C_CUSTKEY', right_on='O_CUSTKEY')
filtered_orders = merged_data[~merged_data['O_COMMENT'].str.contains('pending|deposits', regex=True, na=False)]
customer_order_count = filtered_orders.groupby('C_CUSTKEY').size().reset_index(name='number_of_orders')
distribution = customer_order_count.groupby('number_of_orders').size().reset_index(name='number_of_customers')

# Write the output to a CSV file
distribution.to_csv('query_output.csv', index=False)
