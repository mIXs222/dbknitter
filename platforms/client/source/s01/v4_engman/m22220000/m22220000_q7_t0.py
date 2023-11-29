import pandas as pd
import pymysql
from direct_redis import DirectRedis

# MySQL Connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Query for MySQL
mysql_query = """
SELECT 
    c.C_NAME AS CUST_NATION, 
    YEAR(o.O_ORDERDATE) AS L_YEAR, 
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS REVENUE,
    s.N_NAME AS SUPP_NATION
FROM
    customer c
JOIN
    orders o ON c.C_CUSTKEY = o.O_CUSTKEY
JOIN
    lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
WHERE
    c.C_NATIONKEY IN ('INDIA', 'JAPAN')
AND
    s.S_NATIONKEY IN ('INDIA', 'JAPAN')
AND 
    c.C_NATIONKEY <> s.S_NATIONKEY
AND 
    YEAR(o.O_ORDERDATE) IN (1995, 1996)
AND 
    l.L_SHIPDATE BETWEEN o.O_ORDERDATE AND DATE_ADD(o.O_ORDERDATE, INTERVAL 2 YEAR)
GROUP BY
    CUST_NATION, L_YEAR, SUPP_NATION
ORDER BY
    SUPP_NATION, CUST_NATION, L_YEAR
"""

# Execute MySQL Query
mysql_df = pd.read_sql(mysql_query, mysql_conn)

# Redis Connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Query for Redis (Fetching data from Redis)
nation_df = pd.read_json(redis_conn.get('nation').decode())
supplier_df = pd.read_json(redis_conn.get('supplier').decode())

# Merging Redis data to apply the conditions
nation_supplier_df = nation_df.merge(supplier_df, left_on='N_NATIONKEY', right_on='S_NATIONKEY')

# Appending the relevant data from Redis to the MySQL DataFrame
final_df = mysql_df.merge(nation_supplier_df[['N_NAME', 'S_NAME']], left_on=['CUST_NATION', 'SUPP_NATION'], right_on=['N_NAME', 'S_NAME'])

# Write the result to CSV
final_df[['CUST_NATION', 'L_YEAR', 'REVENUE', 'SUPP_NATION']].to_csv('query_output.csv', index=False)

mysql_conn.close()
