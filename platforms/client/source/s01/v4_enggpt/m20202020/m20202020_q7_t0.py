# File: generate_report.py

import pandas as pd
import pymysql
from direct_redis import DirectRedis

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch')

# MySQL Query
mysql_query = """
SELECT 
    s.S_NATIONKEY AS SUPPLIER_NATIONKEY, 
    c.C_NATIONKEY AS CUSTOMER_NATIONKEY, 
    YEAR(l.L_SHIPDATE) AS YEAR_SHIPPED, 
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS REVENUE
FROM 
    supplier AS s
JOIN 
    lineitem AS l ON s.S_SUPPKEY = l.L_SUPPKEY
JOIN 
    customer AS c ON l.L_ORDERKEY IN (SELECT O_ORDERKEY FROM orders WHERE C_CUSTKEY = O_CUSTKEY)
WHERE 
    s.S_NATIONKEY IN (SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'JAPAN' OR N_NAME = 'INDIA') AND
    c.C_NATIONKEY IN (SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'JAPAN' OR N_NAME = 'INDIA') AND
    l.L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'
GROUP BY 
    SUPPLIER_NATIONKEY, CUSTOMER_NATIONKEY, YEAR_SHIPPED
ORDER BY 
    SUPPLIER_NATIONKEY, CUSTOMER_NATIONKEY, YEAR_SHIPPED;
"""

# Run MySQL Query
df_mysql = pd.read_sql_query(mysql_query, mysql_connection)

# Close MySQL connection
mysql_connection.close()

# Connect to Redis
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Get nation data from Redis
nation_df = pd.read_json(redis_connection.get('nation').decode('utf-8'))

# Get orders data from Redis
orders_df = pd.read_json(redis_connection.get('orders').decode('utf-8'))

# Combine data
df_combined = df_mysql.merge(nation_df, left_on='SUPPLIER_NATIONKEY', right_on='N_NATIONKEY')
df_combined = df_combined.merge(nation_df, left_on='CUSTOMER_NATIONKEY', right_on='N_NATIONKEY', suffixes=('_SUPPLIER', '_CUSTOMER'))
df_combined = df_combined.merge(orders_df, left_on='ORDERKEY', right_on='O_ORDERKEY')

# Write to CSV
df_combined.to_csv('query_output.csv', index=False)
