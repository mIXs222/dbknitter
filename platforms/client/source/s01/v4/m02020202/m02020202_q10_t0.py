# query_data.py
import pandas as pd
import pymysql
from direct_redis import DirectRedis

# Define the connection details for MySQL and Redis
mysql_connection = {
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
    'database': 'tpch',
}

# Connect to MySQL
connection = pymysql.connect(**mysql_connection)
cursor = connection.cursor()

# Query for MySQL
mysql_query = """
SELECT
    O_CUSTKEY,
    SUM(O_TOTALPRICE) AS REVENUE,
    N_NAME
FROM
    orders,
    nation
WHERE
    O_ORDERDATE >= '1993-10-01'
    AND O_ORDERDATE < '1994-01-01'
    AND N_NATIONKEY = 'nation key placeholder'  -- Replace with actual nation key later
GROUP BY
    O_CUSTKEY,
    N_NAME
"""

# Replace - nation key placeholder with actual nation keys from Redis
# Since nation table doesn't exist in MySQL in this scenario

# Execute MySQL query
cursor.execute(mysql_query)
order_data = cursor.fetchall()

# Convert order data to DataFrame
orders_df = pd.DataFrame(order_data, columns=['C_CUSTKEY', 'REVENUE', 'N_NAME'])

# Close MySQL connection
cursor.close()
connection.close()

# Connect to Redis
redis_connection_detail = {
    'host': 'redis',
    'port': 6379,
    'db': 0,
}
redis_client = DirectRedis(**redis_connection_detail)

# Get customer and lineitem data from Redis
customers_df = pd.read_json(redis_client.get('customer'))
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Merge DataFrames
result_df = pd.merge(customers_df, orders_df, on='C_CUSTKEY')
result_df = pd.merge(result_df, lineitem_df, left_on='C_CUSTKEY', right_on='L_ORDERKEY')

# Filter by conditions and calculate REVENUE
result_df = result_df[result_df['L_RETURNFLAG'] == 'R']
result_df['REVENUE'] *= (1 - result_df['L_DISCOUNT'])

# Group and sort for final output
output_df = result_df.groupby(['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_PHONE', 'N_NAME', 'C_ADDRESS', 'C_COMMENT']).agg({'REVENUE': 'sum'}).reset_index()
output_df = output_df.sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[False, True, True, False])

# Write result to CSV
output_df.to_csv('query_output.csv', index=False)
