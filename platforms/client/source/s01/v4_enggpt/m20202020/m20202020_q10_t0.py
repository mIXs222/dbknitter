# importing necessary modules
import pymysql
import pandas as pd
import direct_redis

# connection information for MySQL
mysql_db_name = "tpch"
mysql_username = "root"
mysql_password = "my-secret-pw"
mysql_hostname = "mysql"

# connection information for Redis
redis_db_name = "0"
redis_port = 6379
redis_hostname = "redis"

# connect to MySQL
mysql_conn = pymysql.connect(host=mysql_hostname, user=mysql_username, passwd=mysql_password, db=mysql_db_name)
mysql_cursor = mysql_conn.cursor()

# execute the MySQL portion of the query
mysql_query = """
SELECT 
    c.C_CUSTKEY,
    c.C_NAME,
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS REVENUE,
    c.C_ACCTBAL,
    n.N_NAME,
    c.C_ADDRESS,
    c.C_PHONE,
    c.C_COMMENT
FROM 
    customer c
JOIN 
    lineitem l ON c.C_CUSTKEY = l.L_ORDERKEY
WHERE 
    l.L_RETURNFLAG = 'R'
GROUP BY 
    c.C_CUSTKEY, c.C_NAME, c.C_ACCTBAL, c.C_ADDRESS, c.C_PHONE, c.C_COMMENT, n.N_NAME
ORDER BY 
    REVENUE ASC, c.C_CUSTKEY, c.C_NAME, c.C_ACCTBAL DESC;
"""
mysql_cursor.execute(mysql_query)
mysql_results = pd.DataFrame(mysql_cursor.fetchall(), columns=[
    'C_CUSTKEY', 'C_NAME', 'REVENUE', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT'
])
mysql_cursor.close()
mysql_conn.close()

# connect to Redis
redis_client = direct_redis.DirectRedis(host=redis_hostname, port=redis_port, db=redis_db_name)
nation_df = pd.read_pickle(redis_client.get('nation'))
orders_df = pd.read_pickle(redis_client.get('orders'))

# Perform the Redis part of the query
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
filtered_orders = orders_df[
    (orders_df['O_ORDERDATE'] >= '1993-10-01') & 
    (orders_df['O_ORDERDATE'] <= '1993-12-31')
]

# Merge results
merged_results = pd.merge(
    mysql_results,
    filtered_orders,
    how='inner',
    left_on='C_CUSTKEY',
    right_on='O_CUSTKEY'
).drop(['O_CUSTKEY'], axis=1)
final_results = pd.merge(
    merged_results,
    nation_df,
    how='left',
    left_on='C_NATIONKEY',
    right_on='N_NATIONKEY'
).drop(['C_NATIONKEY', 'N_NATIONKEY'], axis=1)

# Output to CSV
final_results.to_csv('query_output.csv', index=False)
