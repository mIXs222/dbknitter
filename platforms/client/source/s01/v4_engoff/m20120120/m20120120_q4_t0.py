import pymysql
import pandas as pd
from sqlalchemy import create_engine
import direct_redis

# Function to get data from MySQL
def get_mysql_data():
    connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
    try:
        lineitem_query = """
        SELECT L_ORDERKEY, L_COMMITDATE, L_RECEIPTDATE
        FROM lineitem
        WHERE L_COMMITDATE < L_RECEIPTDATE
        """
        df_lineitem = pd.read_sql(lineitem_query, connection)
        return df_lineitem
    finally:
        connection.close()

# Function to get data from Redis
def get_redis_data():
    r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    df_orders = pd.read_json(r.get('orders'), orient='records')
    df_orders = df_orders[(df_orders['O_ORDERDATE'] >= '1993-07-01') & (df_orders['O_ORDERDATE'] <= '1993-10-01')]
    return df_orders

# Get data from MySQL
df_lineitem = get_mysql_data()

# Get data from Redis
df_orders = get_redis_data()

# Merge Redis and MySQL data
df_merged = pd.merge(df_orders, df_lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Aggregate the results
result = df_merged.groupby('O_ORDERPRIORITY').size().reset_index(name='order_count')

# Sort the result
result = result.sort_values(by='O_ORDERPRIORITY')

# Write the result to CSV
result.to_csv('query_output.csv', index=False)
