# query_executor.py
import pymysql
import pandas as pd
from direct_redis import DirectRedis

def mysql_connection():
    return pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')

def query_mysql(query, connection):
    return pd.read_sql_query(query, connection)

def query_redis(key):
    redis_client = DirectRedis(host='redis', port=6379, db=0)
    return pd.read_json(redis_client.get(key))

# Connect to MySQL and fetch data
try:
    mysql_conn = mysql_connection()
    customer_orders = query_mysql('''
        SELECT C_CUSTKEY, O_ORDERKEY, L_EXTENDEDPRICE, L_DISCOUNT
        FROM customer
        JOIN orders ON C_CUSTKEY = O_CUSTKEY
        JOIN lineitem ON O_ORDERKEY = L_ORDERKEY
        WHERE O_ORDERDATE >= '1990-01-01' AND O_ORDERDATE < '1995-01-01'
    ''', mysql_conn)

    suppliers = query_mysql('''
        SELECT S_SUPPKEY, S_NATIONKEY FROM supplier
    ''', mysql_conn)

finally:
    mysql_conn.close()

# Fetch data from Redis
nation = query_redis('nation').set_index('N_NATIONKEY')
region = query_redis('region').set_index('R_REGIONKEY')

# Process DataFrames to get the final result
merged_df = customer_orders.merge(suppliers, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
merged_df = merged_df.merge(nation, left_on='S_NATIONKEY', right_index=True)
merged_df = merged_df.merge(region, left_on='N_REGIONKEY', right_index=True)

# Filter for ASIA region
filtered_df = merged_df[merged_df['R_NAME'] == 'ASIA']

# Compute revenue
filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])
result_df = filtered_df.groupby('N_NAME')['REVENUE'].sum().reset_index()

# Order by revenue desc
result_df = result_df.sort_values(by='REVENUE', ascending=False)

# Save to CSV
result_df.to_csv('query_output.csv', index=False)

