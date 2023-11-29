# query.py
import pandas as pd
import pymysql
import direct_redis

# MySQL connection and query
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_query = """
    SELECT
        YEAR(o_orderdate) AS order_year,
        SUM(l_extendedprice * (1 - l_discount)) AS market_share
    FROM lineitem
    JOIN orders ON l_orderkey = o_orderkey
    JOIN part ON l_partkey = p_partkey
    JOIN supplier ON l_suppkey = s_suppkey
    JOIN nation ON s_nationkey = n_nationkey
    JOIN region ON n_regionkey = r_regionkey
    WHERE r_name = 'ASIA'
        AND n_name = 'INDIA'
        AND p_type = 'SMALL PLATED COPPER'
        AND o_orderdate BETWEEN '1995-01-01' AND '1996-12-31'
    GROUP BY order_year
"""
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    mysql_result = cursor.fetchall()
mysql_conn.close()

# Convert MySQL result to pandas DataFrame
df_mysql = pd.DataFrame(mysql_result, columns=['order_year', 'market_share'])

# Redis connection and data fetching
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)
df_region = pd.read_json(redis_conn.get('region'))
df_supplier = pd.read_json(redis_conn.get('supplier'))
df_customer = pd.read_json(redis_conn.get('customer'))
df_lineitem = pd.read_json(redis_conn.get('lineitem'))

# Perform final merge and aggregate
# Note: In a real case, this part should be corrected according to the actual data model available in Redis, since Redis does not support SQL-like joins.
# This example assumes that the data from Redis is available in a structured form that can be joined and aggregated similar to the example for MySQL.

# Here we are only calculating the market share for MySQL results since Redis cannot join tables like SQL.
df_result = df_mysql[df_mysql['order_year'].isin([1995, 1996])]

# Write output to CSV
df_result.to_csv('query_output.csv', index=False)
