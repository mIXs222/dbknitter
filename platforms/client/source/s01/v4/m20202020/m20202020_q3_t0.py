import pymysql
import pandas as pd
import direct_redis

# Connect to MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Define queries for MySQL - customer and lineitem tables
mysql_query_customer = """
SELECT C_CUSTKEY, C_MKTSEGMENT 
FROM customer 
WHERE C_MKTSEGMENT = 'BUILDING'
"""

mysql_query_lineitem = """
SELECT L_ORDERKEY, 
       L_EXTENDEDPRICE, 
       L_DISCOUNT 
FROM lineitem 
WHERE L_SHIPDATE > '1995-03-15'
"""

# Execute MySQL queries
customer_df = pd.read_sql(mysql_query_customer, mysql_conn)
lineitem_df = pd.read_sql(mysql_query_lineitem, mysql_conn)

# Close MySQL connection
mysql_conn.close()

# Connect to Redis using direct_redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get orders data from Redis
orders_df = pd.DataFrame(redis_conn.get('orders'))

# Perform required operations with Redis data
orders_df = orders_df[(orders_df['O_ORDERDATE'] < '1995-03-15')]

# Merge dataframes to simulate the join operation
result_df = customer_df.merge(orders_df, how='inner', left_on='C_CUSTKEY', right_on='O_CUSTKEY')
result_df = result_df.merge(lineitem_df, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Calculate revenue
result_df['REVENUE'] = result_df['L_EXTENDEDPRICE'] * (1 - result_df['L_DISCOUNT'])

# Group by and sort as per the query
grouped_result = result_df.groupby(['L_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY']).agg({
    'REVENUE': 'sum'
}).reset_index()

sorted_result = grouped_result.sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True])

# Write the result to csv file
sorted_result.to_csv('query_output.csv', index=False)

print("Query result written to query_output.csv")
