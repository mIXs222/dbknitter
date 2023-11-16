# query_exec.py
import pymysql
import pandas as pd
from sqlalchemy import create_engine
import direct_redis

# Function to connect to MySQL
def connect_mysql(hostname, username, password, database_name):
    connection = pymysql.connect(host=hostname, user=username, passwd=password, db=database_name)
    return connection

# Function to query MySQL database
def query_mysql(connection, query):
    return pd.read_sql(query, connection)

# Function to connect to Redis and get DataFrame
def get_redis_dataframe(hostname, port, tablename):
    r = direct_redis.DirectRedis(host=hostname, port=port)
    return r.get(tablename)

# MySQL connection information
mysql_info = {
    'hostname': 'mysql',
    'username': 'root',
    'password': 'my-secret-pw',
    'database_name': 'tpch'
}

# Redis connection information
redis_info = {
    'hostname': 'redis',
    'port': 6379
}

# Connect to MySQL
mysql_conn = connect_mysql(mysql_info['hostname'], mysql_info['username'], mysql_info['password'], mysql_info['database_name'])

# SQL Query for the supplier table
supplier_query = """
SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE
FROM supplier
"""

# Get the supplier DataFrame
supplier_df = query_mysql(mysql_conn, supplier_query)

# Get the lineitem DataFrame
lineitem_df = get_redis_dataframe(redis_info['hostname'], redis_info['port'], 'lineitem')

# Calculate TOTAL_REVENUE with lineitem DataFrame
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
lineitem_df = lineitem_df[(lineitem_df['L_SHIPDATE'] >= '1996-01-01') & (lineitem_df['L_SHIPDATE'] < '1996-04-01')]
lineitem_df['TOTAL_REVENUE'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])
revenue_df = lineitem_df.groupby('L_SUPPKEY')['TOTAL_REVENUE'].sum().reset_index()

# Find the supplier with the maximum revenue
max_revenue = revenue_df['TOTAL_REVENUE'].max()
max_revenue_supplier = revenue_df[revenue_df['TOTAL_REVENUE'] == max_revenue]

# Combine data from supplier and revenue DataFrame
result_df = supplier_df.merge(max_revenue_supplier, left_on='S_SUPPKEY', right_on='L_SUPPKEY')

# Write the final result to a CSV file
result_df.to_csv('query_output.csv', index=False)

# Close the MySQL connection
mysql_conn.close()
