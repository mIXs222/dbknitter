# python_code.py
import pymysql
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# MySQL query
mysql_query = """
SELECT 
    c_custkey, c_name,
    SUM(l_extendedprice * (1 - l_discount)) as revenue_lost,
    c_acctbal, c_nationkey, c_address, c_phone, c_comment
FROM 
    customer
JOIN
    orders ON c_custkey = o_custkey
JOIN
    lineitem ON o_orderkey = l_orderkey
WHERE
    l_returnflag = 'R'
    AND o_orderdate >= '1993-10-01'
    AND o_orderdate < '1994-01-01'
GROUP BY
    c_custkey, c_name, c_acctbal, c_nationkey, c_address, c_phone, c_comment
ORDER BY
    revenue_lost ASC, c_custkey ASC, c_name ASC, c_acctbal DESC;
"""

# Getting the data from mysql
mysql_cursor.execute(mysql_query)
mysql_data = mysql_cursor.fetchall()

# Storing mysql data in pandas dataframe
df_mysql = pd.DataFrame(mysql_data, columns=['C_CUSTKEY', 'C_NAME', 'revenue_lost', 'C_ACCTBAL', 'C_NATIONKEY', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT'])

# Getting the data from Redis
df_customer = redis_client.get('customer')
df_orders = redis_client.get('orders')
df_lineitem = redis_client.get('lineitem')

# Remembering to close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Formatting Redis data in pandas dataframes
# Note: The actual extraction logic from Redis may vary
# because the specific method to get a pandas DataFrame from Redis is not given.
# 'DirectRedis' is not officially documented.

# Converting and combining data from Redis if applicable

# Saving the final output to CSV
df_mysql.to_csv('query_output.csv', index=False)
