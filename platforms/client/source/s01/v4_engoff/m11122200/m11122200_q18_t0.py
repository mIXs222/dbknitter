import pymysql
import direct_redis
import pandas as pd

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql', 
    user='root', 
    password='my-secret-pw', 
    database='tpch'
)

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get the data from MySQL
query = """
SELECT o.O_ORDERKEY, o.O_CUSTKEY, O_ORDERDATE, O_TOTALPRICE, SUM(l.L_QUANTITY) as total_quantity
FROM orders o 
JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
GROUP BY o.O_ORDERKEY, o.O_CUSTKEY, O_ORDERDATE, O_TOTALPRICE
HAVING total_quantity > 300;
"""
mysql_data = pd.read_sql(query, mysql_conn)

# Get the data from Redis and convert to DataFrame
customer_data = pd.DataFrame(redis_conn.get('customer'))
customer_data.columns = ['C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'C_NATIONKEY', 'C_PHONE', 'C_ACCTBAL', 'C_MKTSEGMENT', 'C_COMMENT']

# Merge MySQL and Redis data
result = pd.merge(
    mysql_data,
    customer_data,
    left_on="O_CUSTKEY",
    right_on="C_CUSTKEY"
)[['C_NAME', 'O_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'total_quantity']]

# Write the result to CSV
result.to_csv('query_output.csv', index=False)
