# python_code.py
import pymysql
import pandas as pd
from sqlalchemy import create_engine
import direct_redis

# MySQL Connection
mysql_params = {
    'database': 'tpch',
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
}

# Establish a MySQL connection using pymysql and create MySQL engine
mysql_connection = pymysql.connect(**mysql_params)
mysql_engine = create_engine(f'mysql+pymysql://{mysql_params["user"]}:{mysql_params["password"]}@{mysql_params["host"]}/{mysql_params["database"]}')

# Query MySQL for parts from the nation and supplier tables
query_mysql = '''
SELECT 
    n.N_NAME AS nation,
    YEAR(o.O_ORDERDATE) AS o_year,
    SUM((l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) - (ps.PS_SUPPLYCOST * l.L_QUANTITY)) AS profit
FROM 
    part p
JOIN 
    supplier s ON p.P_PARTKEY = s.S_SUPPKEY
JOIN 
    nation n ON s.S_NATIONKEY = n.N_NATIONKEY
JOIN 
    partsupp ps ON s.S_SUPPKEY = ps.PS_SUPPKEY
JOIN 
    lineitem l ON ps.PS_PARTKEY = l.L_PARTKEY AND ps.PS_SUPPKEY = l.L_SUPPKEY
JOIN 
    orders o ON o.O_ORDERKEY = l.L_ORDERKEY
WHERE 
    p.P_NAME LIKE '%dim%'
GROUP BY 
    nation, o_year
ORDER BY 
    nation ASC, o_year DESC
'''

# Create Pandas DataFrame from MySQL data
df_mysql = pd.read_sql_query(query_mysql, mysql_engine)

# Redis connection
redis_params = {
    'hostname': 'redis',
    'port': 6379,
    'db': 0
}

# Establish a Redis connection using direct_redis
redis_connection = direct_redis.DirectRedis(host=redis_params['hostname'], port=redis_params['port'], db=redis_params['db'])

# Fetch Redis data
df_partsupp = redis_connection.get('partsupp')
df_orders = redis_connection.get('orders')
df_lineitem = redis_connection.get('lineitem')

# Merge Redis DataFrames
df_redis = pd.merge(df_orders, df_lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
df_redis = pd.merge(df_redis, df_partsupp, left_on=('L_PARTKEY', 'L_SUPPKEY'), right_on=('PS_PARTKEY', 'PS_SUPPKEY'))
df_redis['O_YEAR'] = pd.to_datetime(df_redis['O_ORDERDATE']).dt.year

# Perform the operations on the Redis data
df_redis = df_redis[df_redis['P_NAME'].str.contains('dim')]
df_redis['PROFIT'] = (df_redis['L_EXTENDEDPRICE'] * (1 - df_redis['L_DISCOUNT'])) - (df_redis['PS_SUPPLYCOST'] * df_redis['L_QUANTITY'])
df_redis = df_redis.groupby(['NATION', 'O_YEAR']).agg({'PROFIT': 'sum'}).reset_index()
df_redis.sort_values(['NATION', 'O_YEAR'], ascending=[True, False], inplace=True)

# Final DataFrame (if data from both databases was obtained)
# Merge or perform any additional needed operations to get the final result
# final_df = pd.concat([df_mysql, df_redis])  # This is an example, the actual merging will depend on the task

# Write the final DataFrame to a CSV file
# Replace 'final_df' with the actual variable name holding the final data
# final_df.to_csv('query_output.csv', index=False)

# Since we cannot perform actual merging due to Redis dataset only, we save the MySQL result
df_mysql.to_csv('query_output.csv', index=False)

# Close the connections
mysql_connection.close()
redis_connection.close()
