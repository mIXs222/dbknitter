import csv
import pandas as pd
import pymysql
from direct_redis import DirectRedis

# Function to execute an SQL query and return the results as a DataFrame
def execute_sql_query(sql_query, connection_params):
    conn = pymysql.connect(
        host=connection_params['hostname'],
        user=connection_params['username'],
        password=connection_params['password'],
        database=connection_params['database']
    )
    with conn.cursor() as cursor:
        cursor.execute(sql_query)
        data = cursor.fetchall()
    conn.close()
    return pd.DataFrame(list(data))

# Function to get DataTable from redis and return as a DataFrame
def get_table_from_redis(tablename, redis_params):
    redis_connection = DirectRedis(host=redis_params['hostname'], port=redis_params['port'])
    return pd.read_json(redis_connection.get(tablename))

# MySQL connection parameters
mysql_params = {
    'hostname': 'mysql',
    'username': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}

# Redis connection parameters
redis_params = {
    'hostname': 'redis',
    'port': 6379
}

# Retrieve lineitem and orders from mysql
lineitem = execute_sql_query('SELECT * FROM lineitem', mysql_params)
orders = execute_sql_query('SELECT * FROM orders', mysql_params)

# Retrieve nation from redis
nation = get_table_from_redis('nation', redis_params)

# Filter required data
orders = orders[(orders[2] == 'F')]  # O_ORDERSTATUS is at index 2
lineitem_filtered = lineitem[lineitem[8] > lineitem[11]]  # L_RECEIPTDATE is at index 8, L_COMMITDATE is at index 11

# Join the DataFrames
result = (lineitem_filtered
          .merge(orders, left_on=0, right_on=0)  # L_ORDERKEY and O_ORDERKEY are both at index 0
          .merge(nation[nation[1] == 'SAUDI ARABIA'], left_on=2, right_on=0, suffixes=('', '_nation')) # L_SUPPKEY is at index 2, N_NATIONKEY at index 0
          )

# Aggregate the results
final = (result.groupby(19)  # S_NAME is at index 19
         .agg(NUMWAIT=('L_ORDERKEY', 'count'),
              S_NAME=('S_NAME', 'first'))
         .reset_index(drop=True)
         .sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])
         )

# Write output to CSV
final.to_csv('query_output.csv', index=False)
