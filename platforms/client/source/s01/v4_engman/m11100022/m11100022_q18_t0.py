import pymysql
import pandas as pd
from sqlalchemy import create_engine
import direct_redis

# Connect to MySQL
my_sql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# MySQL query
mysql_query = """
SELECT c.C_NAME, c.C_CUSTKEY, o.O_ORDERKEY, o.O_ORDERDATE, o.O_TOTALPRICE
FROM customer AS c
JOIN orders AS o ON c.C_CUSTKEY = o.O_CUSTKEY
WHERE o.O_TOTALPRICE > 300
ORDER BY o.O_TOTALPRICE DESC, o.O_ORDERDATE
"""

# Execute MySQL query and fetch data
with my_sql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    mysql_results = cursor.fetchall()

# Convert MySQL data to DataFrame
mysql_df = pd.DataFrame(mysql_results, columns=['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE'])

# Close MySQL connection
my_sql_conn.close()

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch orders and lineitem tables from Redis
orders_df = pd.read_json(redis_conn.get('orders'))
lineitem_df = pd.read_json(redis_conn.get('lineitem'))

# Processing Redis data: filter orders with quantity > 300 and select necessary columns
large_orders = lineitem_df.groupby('L_ORDERKEY').agg({'L_QUANTITY': 'sum'})
large_orders = large_orders[large_orders['L_QUANTITY'] > 300].reset_index()
filtered_orders_df = orders_df.merge(large_orders, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Merge MySQL and Redis data
final_df = mysql_df.merge(filtered_orders_df, left_on='O_ORDERKEY', right_on='O_ORDERKEY')

# Sorting the final DataFrame
final_df.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True], inplace=True)

# Selecting relevant columns
final_df = final_df[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'L_QUANTITY']]

# Write the result to CSV
final_df.to_csv('query_output.csv', index=False)
