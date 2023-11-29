import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Execute the query on the orders table in MySQL
mysql_query = """
    SELECT O_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE
    FROM orders
    WHERE O_ORDERKEY IN (
        SELECT L_ORDERKEY
        FROM lineitem
        GROUP BY L_ORDERKEY
        HAVING SUM(L_QUANTITY) > 300
    )
"""
mysql_df = pd.read_sql(mysql_query, mysql_conn)

# Close MySQL connection
mysql_conn.close()

# Connect to Redis database
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get the customer and lineitem dataframes from Redis
customer_df = pd.read_json(redis_client.get('customer'))
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Perform the join operation and filtering
result_df = (
    mysql_df.merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
    .merge(lineitem_df[['L_ORDERKEY', 'L_QUANTITY']], left_on='O_ORDERKEY', right_on='L_ORDERKEY')
)

# Filter orders with total quantity larger than 300
result_df = result_df.groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE']).filter(
    lambda x: x['L_QUANTITY'].sum() > 300
)

# Select necessary columns
result_df = result_df[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE']]

# Order by total price descending, order date ascending
result_df = result_df.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])

# Write result to CSV
result_df.to_csv('query_output.csv', index=False)
