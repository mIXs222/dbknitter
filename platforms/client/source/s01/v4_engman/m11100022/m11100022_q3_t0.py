import pandas as pd
import pymysql
from datetime import datetime
from direct_redis import DirectRedis

# Establish a connection to MySQL database
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

# Prepare the query for MySQL
mysql_query = """
SELECT C_CUSTKEY, C_MKTSEGMENT
FROM customer
WHERE C_MKTSEGMENT = 'BUILDING';
"""

# Execute the query
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    mysql_results = cursor.fetchall()

# Fetch customer data and create a DataFrame
customer_df = pd.DataFrame(mysql_results, columns=['C_CUSTKEY', 'C_MKTSEGMENT'])

# Close the MySQL connection
mysql_conn.close()

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Fetch orders and lineitem data from Redis as DataFrames
orders_df = pd.read_json(redis_client.get('orders'), orient='records')
lineitem_df = pd.read_json(redis_client.get('lineitem'), orient='records')

# Merge the DataFrames
merged_df = orders_df.merge(lineitem_df, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')
merged_df = merged_df.merge(customer_df, how='inner', left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Convert date strings to datetime objects for comparison
merged_df['O_ORDERDATE'] = pd.to_datetime(merged_df['O_ORDERDATE'])
merged_df['L_SHIPDATE'] = pd.to_datetime(merged_df['L_SHIPDATE'])

# Filter the DataFrame
filtered_df = merged_df[
    (merged_df['C_MKTSEGMENT'] == 'BUILDING') &
    (merged_df['O_ORDERDATE'] < datetime(1995, 3, 5)) &
    (merged_df['L_SHIPDATE'] > datetime(1995, 3, 15))
]

# Calculate the revenue
filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])

# Select and sort the required columns and data
result_df = filtered_df[['O_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY']]
result_df = result_df.sort_values(by='REVENUE', ascending=False)

# Write the result to a CSV file
result_df.to_csv('query_output.csv', index=False)
