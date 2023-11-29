import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Function to query MySQL and return a Pandas DataFrame
def query_mysql(query, connection_params):
    conn = pymysql.connect(**connection_params)
    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()

# Function to query Redis and return a Pandas DataFrame
def query_redis(key, host, port):
    redis_conn = DirectRedis(host=host, port=port)
    return pd.read_json(redis_conn.get(key))

# Connect to MySQL to get 'nation' and 'orders' tables
mysql_connection_parameters = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}

# Fetch nation and orders data from MySQL
nation_query = "SELECT N_NATIONKEY, N_NAME FROM nation;"
orders_query = """
SELECT O_ORDERKEY, O_CUSTKEY
FROM orders
WHERE O_ORDERDATE >= '1993-10-01' AND O_ORDERDATE < '1994-01-01';
"""
nation_df = query_mysql(nation_query, mysql_connection_parameters)
orders_df = query_mysql(orders_query, mysql_connection_parameters)

# Connect to Redis
redis_host = 'redis'
redis_port = 6379

# Fetch lineitem and customer data from Redis
lineitem_df = query_redis('lineitem', redis_host, redis_port)
customer_df = query_redis('customer', redis_host, redis_port)

# Filter lineitem for returned parts
lineitem_returns_df = lineitem_df[lineitem_df['L_RETURNFLAG'] == 'R']

# Calculate revenue lost from lineitems
lineitem_returns_df['REVENUE_LOST'] = lineitem_returns_df['L_EXTENDEDPRICE'] * (1 - lineitem_returns_df['L_DISCOUNT'])

# Join lineitem and orders on order key
orders_returns_df = pd.merge(orders_df, lineitem_returns_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Group by customer key and sum the revenue lost
revenue_loss_per_customer = orders_returns_df.groupby('O_CUSTKEY')['REVENUE_LOST'].sum().reset_index()

# Join with customer and nation tables
final_df = pd.merge(customer_df, revenue_loss_per_customer, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
final_df = pd.merge(final_df, nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Select final columns and sort as per requirement
output_df = final_df[['O_CUSTKEY', 'C_NAME', 'REVENUE_LOST', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']] \
    .sort_values(by=['REVENUE_LOST', 'O_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[True, True, True, False])

# Output to CSV
output_df.to_csv('query_output.csv', index=False)
