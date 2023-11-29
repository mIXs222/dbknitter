# query_execution.py
import pymysql
import pandas as pd
import direct_redis

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
)

# Query MySQL for orders
orders_query = """
SELECT O_CUSTKEY, COUNT(*) as num_orders 
FROM orders 
WHERE O_ORDERSTATUS NOT LIKE '%pending%' 
AND O_COMMENT NOT LIKE '%deposits%' 
GROUP BY O_CUSTKEY;
"""
orders_df = pd.read_sql(orders_query, mysql_connection)
mysql_connection.close()

# Connect to Redis
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)
customer_df = pd.read_json(redis_connection.get('customer'))

# Merge data on customer key
merged_df = customer_df.merge(orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='inner')

# Group by number of orders and count number of customers
result = merged_df.groupby('num_orders').size().reset_index(name='num_customers')

# Save to csv
result.to_csv('query_output.csv', index=False)
