import pymysql
import pandas as pd
import direct_redis

# Connect to MySQL database
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch')

# Query to select relevant data from the mysqldatabase
mysql_query = """
SELECT 
    c.C_CUSTKEY, c.C_NAME, c.C_ADDRESS, c.C_PHONE, c.C_ACCTBAL, c.C_COMMENT,
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS revenue_lost
FROM 
    customer c
JOIN 
    lineitem l ON c.C_CUSTKEY = l.L_ORDERKEY
WHERE 
    l.L_RETURNFLAG = 'R'
AND 
    l.L_SHIPDATE BETWEEN '1993-10-01' AND '1994-01-01'
GROUP BY c.C_CUSTKEY
ORDER BY revenue_lost DESC, c.C_CUSTKEY, c.C_NAME, c.C_ACCTBAL;
"""

# Execute the MySQL query and get the data
mysql_df = pd.read_sql(mysql_query, mysql_connection)

# Close mysql connection
mysql_connection.close()

# Connect to Redis using direct_redis
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get nation data from Redis
nation_df = pd.read_json(redis_connection.get('nation'))

# Merge MySQL and Redis data
merged_df = pd.merge(mysql_df, nation_df, left_on='C_CUSTKEY', right_on='N_NATIONKEY', how='inner')

# Select required columns
result_df = merged_df[['C_NAME', 'C_ADDRESS', 'N_NAME', 'C_PHONE', 'C_ACCTBAL', 'C_COMMENT', 'revenue_lost']]

# Write the result to a CSV file
result_df.to_csv('query_output.csv', index=False)
