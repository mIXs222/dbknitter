# query_code.py
import pymysql
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import direct_redis

# Connection details for MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch')
# Create SQLite engine in memory to combine table results from different DBs
sqlite_engine = create_engine('sqlite:///:memory:')

# Load lineitem from MySQL into Pandas DataFrame
query_lineitem = """
SELECT
  L_SUPPKEY,
  L_EXTENDEDPRICE,
  L_DISCOUNT,
  L_SHIPDATE
FROM lineitem
WHERE
  L_SHIPDATE >= '1996-01-01'
  AND L_SHIPDATE < '1996-04-01'
"""
df_lineitem = pd.read_sql(query_lineitem, mysql_connection)
mysql_connection.close()  # Close MySQL connection

# Compute revenue
df_lineitem['TOTAL_REVENUE'] = df_lineitem['L_EXTENDEDPRICE'] * (1 - df_lineitem['L_DISCOUNT'])
df_revenue = df_lineitem.groupby('L_SUPPKEY')['TOTAL_REVENUE'].sum().reset_index()

# Write revenue DataFrame to SQLite
df_revenue.to_sql('revenue0', sqlite_engine, index=False, if_exists='replace')

# Connection details for Redis
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Load supplier from Redis into Pandas DataFrame
df_supplier = pd.read_json(redis_connection.get('supplier'))

# Write supplier DataFrame to SQLite
df_supplier.to_sql('supplier', sqlite_engine, index=False, if_exists='replace')

# Execute the combined query in SQLite
combined_query = """
WITH max_revenue AS (
  SELECT
    MAX(TOTAL_REVENUE) as MAX_REVENUE
  FROM
    revenue0
)
SELECT
  s.S_SUPPKEY,
  s.S_NAME,
  s.S_ADDRESS,
  s.S_PHONE,
  r.TOTAL_REVENUE
FROM
  supplier s,
  revenue0 r,
  max_revenue
WHERE
  s.S_SUPPKEY = r.L_SUPPKEY
  AND r.TOTAL_REVENUE = max_revenue.MAX_REVENUE
ORDER BY
  s.S_SUPPKEY
"""
df_result = pd.read_sql(combined_query, sqlite_engine)

# Write result to CSV
df_result.to_csv('query_output.csv', index=False)
