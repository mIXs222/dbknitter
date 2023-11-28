# Import required libraries
import pymysql
import pandas as pd
import direct_redis

# Connect to MySQL database
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Query to select relevant data from MySQL tables - region, supplier, customer, lineitem
mysql_query = """
SELECT 
    n.N_NAME,
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS total_revenue
FROM 
    customer AS c
JOIN 
    orders AS o ON c.C_CUSTKEY = o.O_CUSTKEY
JOIN 
    lineitem AS l ON o.O_ORDERKEY = l.L_ORDERKEY
JOIN 
    supplier AS s ON l.L_SUPPKEY = s.S_SUPPKEY
JOIN 
    nation AS n ON s.S_NATIONKEY = n.N_NATIONKEY AND c.C_NATIONKEY = n.N_NATIONKEY
JOIN 
    region AS r ON n.N_REGIONKEY = r.R_REGIONKEY
WHERE 
    r.R_NAME = 'ASIA'
    AND o.O_ORDERDATE >= '1990-01-01'
    AND o.O_ORDERDATE <= '1994-12-31'
GROUP BY 
    n.N_NAME
ORDER BY 
    total_revenue DESC;
"""

# Execute MySQL query and load into DataFrame
with mysql_connection.cursor() as cursor:
    cursor.execute(mysql_query)
    mysql_data = pd.DataFrame(cursor.fetchall(), columns=['N_NAME', 'total_revenue'])

# Close the MySQL connection
mysql_connection.close()

# Connect to Redis database
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve relevant data from Redis tables - nation, orders
nation_df = pd.DataFrame(eval(redis_connection.get('nation')), columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])
orders_df = pd.DataFrame(eval(redis_connection.get('orders')), columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT'])

# Combine the results from MySQL and Redis databases
combined_results = pd.merge(mysql_data, nation_df, left_on='N_NAME', right_on='N_NAME')
combined_results = combined_results.groupby('N_NAME').agg({'total_revenue': 'sum'}).reset_index()

# Write the final output to a CSV file
combined_results.to_csv('query_output.csv', index=False)
