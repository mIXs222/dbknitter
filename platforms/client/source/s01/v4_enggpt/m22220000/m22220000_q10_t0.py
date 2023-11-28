import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   database='tpch')

# MySQL query
mysql_query = """
SELECT 
    c.C_CUSTKEY,
    c.C_NAME,
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS REVENUE,
    c.C_ACCTBAL,
    c.C_ADDRESS,
    c.C_PHONE,
    c.C_COMMENT
FROM 
    customer c
JOIN 
    orders o ON c.C_CUSTKEY = o.O_CUSTKEY
JOIN 
    lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
WHERE 
    o.O_ORDERDATE >= '1993-10-01' AND o.O_ORDERDATE <= '1993-12-31'
    AND l.L_RETURNFLAG = 'R'
GROUP BY 
    c.C_CUSTKEY, c.C_NAME, c.C_ACCTBAL, c.C_ADDRESS, c.C_PHONE, c.C_COMMENT
ORDER BY 
    REVENUE ASC, c.C_CUSTKEY, c.C_NAME, c.C_ACCTBAL DESC;
"""

with mysql_connection.cursor() as cursor:
    cursor.execute(mysql_query)
    result_set = cursor.fetchall()

# Close the MySQL connection
mysql_connection.close()

# Convert result set to pandas DataFrame
mysql_columns = ['C_CUSTKEY', 'C_NAME', 'REVENUE', 'C_ACCTBAL', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']
df_mysql = pd.DataFrame(list(result_set), columns=mysql_columns)

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Read nation DataFrame from Redis
df_nation = pd.read_msgpack(redis_client.get('nation'))

# Merge DataFrames on nation key
merged_df = pd.merge(df_mysql, df_nation, left_on='C_CUSTKEY', right_on='N_NATIONKEY')

# Save the final result to query_output.csv
merged_df.to_csv('query_output.csv', index=False)
