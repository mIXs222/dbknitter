# import necessary libraries
import pandas as pd
import pymysql
import direct_redis

# Connect to mysql
mysql_connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Prepare mysql query
mysql_query = """
SELECT O_ORDERKEY, O_SHIPPRIORITY,
SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS revenue
FROM orders, lineitem
WHERE O_ORDERDATE < '1995-03-15'
AND O_ORDERKEY = L_ORDERKEY
AND L_SHIPDATE > '1995-03-15'
GROUP BY O_ORDERKEY, O_SHIPPRIORITY
ORDER BY revenue DESC
"""
# Execute mysql query
with mysql_connection.cursor() as cursor:
    cursor.execute(mysql_query)
    mysql_orders_data = cursor.fetchall()

# Extract column names
order_column_names = [desc[0] for desc in cursor.description]

# Convert mysql data into DataFrame
orders_df = pd.DataFrame(mysql_orders_data, columns=order_column_names)

# Close mysql connection
mysql_connection.close()

# Connect to redis
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get customer data from redis
customer_df = pd.DataFrame(eval(redis_connection.get('customer')))

# Filter for market segment "BUILDING"
building_customers_df = customer_df[customer_df['C_MKTSEGMENT'] == 'BUILDING']

# Merge results based on customer keys
merged_df = pd.merge(orders_df, building_customers_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY', how='inner')

# Drop extra columns and sort by revenue
final_df = merged_df[['O_ORDERKEY', 'O_SHIPPRIORITY', 'revenue']]
final_df = final_df.sort_values(by='revenue', ascending=False)

# Write to CSV
final_df.to_csv('query_output.csv', index=False)
