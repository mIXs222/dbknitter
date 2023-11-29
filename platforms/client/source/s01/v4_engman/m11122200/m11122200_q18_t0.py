# large_volume_customer_query.py
import pymysql
import pandas as pd
import direct_redis
import csv

# Connect to the MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Execute the query to get orders with total quantity greater than 300 from MySQL
order_query = """
SELECT o.O_CUSTKEY, o.O_ORDERKEY, o.O_ORDERDATE, o.O_TOTALPRICE, SUM(l.L_QUANTITY) as total_quantity
FROM orders o JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
GROUP BY l.L_ORDERKEY
HAVING SUM(l.L_QUANTITY) > 300
ORDER BY o.O_TOTALPRICE DESC, o.O_ORDERDATE ASC;
"""
with mysql_conn.cursor() as cursor:
    cursor.execute(order_query)
    order_results = cursor.fetchall()

# Convert orders query results to DataFrame
orders_df = pd.DataFrame(order_results, columns=['C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'TOTAL_QUANTITY'])

# Connect to the Redis database
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get customers DataFrame from Redis
customers_df_raw = redis_conn.get('customer')
customers_df = pd.DataFrame(customers_df_raw, columns=redis_conn.get_column_labels('customer'))

# Merge DataFrames on customer key
result_df = pd.merge(
    customers_df,
    orders_df,
    left_on='C_CUSTKEY',
    right_on='C_CUSTKEY'
)

# Select relevant columns and write to CSV
final_df = result_df[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'TOTAL_QUANTITY']]
final_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

# Close connections
mysql_conn.close()
redis_conn.close()
