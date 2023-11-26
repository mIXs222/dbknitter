import pymysql
import pandas as pd
from pandas import DataFrame
import direct_redis

# Connect to MySQL database
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Perform query on orders table in MySQL
orders_query = """
SELECT O_CUSTKEY, O_ORDERKEY, O_COMMENT FROM orders WHERE O_COMMENT NOT LIKE '%pending%deposits%'
"""
mysql_cursor.execute(orders_query)
orders_result = mysql_cursor.fetchall()
orders_df = DataFrame(orders_result, columns=['O_CUSTKEY', 'O_ORDERKEY', 'O_COMMENT'])

# Connect to Redis database
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get customer data from Redis
customer_data = redis_conn.get('customer')
customer_df = pd.read_json(customer_data)

# Perform the LEFT OUTER JOIN operation
merged_df = pd.merge(customer_df, orders_df, how='left', left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# Perform GROUP BY operations
grouped_df = merged_df.groupby('C_CUSTKEY').agg(C_COUNT=('O_ORDERKEY', 'count')).reset_index()

# Perform second GROUP BY operation to get the final result
final_result = grouped_df.groupby('C_COUNT').agg(CUSTDIST=('C_COUNT', 'count')).reset_index()

# Sort the results according to the requirements
final_result_sorted = final_result.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False])

# Write the final output to a CSV file
final_result_sorted.to_csv('query_output.csv', index=False)

# Close database connections
mysql_cursor.close()
mysql_conn.close()
redis_conn.close()
