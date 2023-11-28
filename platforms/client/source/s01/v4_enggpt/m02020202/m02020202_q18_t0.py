import pymysql
import pandas as pd
import csv
from direct_redis import DirectRedis

# Connection details for MySQL
mysql_connection_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}

# Connect to MySQL
mysql_conn = pymysql.connect(
    host=mysql_connection_info['host'],
    user=mysql_connection_info['user'],
    password=mysql_connection_info['password'],
    database=mysql_connection_info['database']
)

# SQL query for MySQL
mysql_query = """
SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERDATE, O_TOTALPRICE
FROM orders
WHERE O_ORDERKEY IN (
    SELECT L_ORDERKEY
    FROM lineitem
    GROUP BY L_ORDERKEY
    HAVING SUM(L_QUANTITY) > 300
)
"""

# Execute the query on MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    orders_data = cursor.fetchall()
    orders_df = pd.DataFrame(orders_data, columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERDATE', 'O_TOTALPRICE'])

# Close the MySQL connection
mysql_conn.close()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from Redis
customer_df = pd.read_json(redis_conn.get('customer').decode())
lineitem_df = pd.read_json(redis_conn.get('lineitem').decode())

# Filter lineitem data based on the order keys obtained from the MySQL query
filtered_lineitem_df = lineitem_df[lineitem_df['L_ORDERKEY'].isin(orders_df['O_ORDERKEY'])]

# Compute sum of quantities per order key
total_quantities = filtered_lineitem_df.groupby('L_ORDERKEY').agg({'L_QUANTITY': 'sum'}).reset_index()

# Merge dataframes to get the complete dataset
combined_df = orders_df.merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
combined_df = combined_df.merge(total_quantities, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Select the required columns and sort the data
final_df = combined_df[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'L_QUANTITY']]
final_df.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True], inplace=True)

# Write results to CSV
final_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
