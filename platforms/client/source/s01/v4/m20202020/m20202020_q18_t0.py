# query_script.py
import pandas as pd
import pymysql
import direct_redis

# Function to execute a query on MySQL database
def query_mysql(query, connection_params):
    connection = pymysql.connect(**connection_params)
    with connection.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
    connection.close()
    return pd.DataFrame(result)

# Function to get data from Redis as a DataFrame
def get_redis_df(key, host, port, db_number):
    redis_connection = direct_redis.DirectRedis(host=host, port=port, db=db_number)
    df = pd.read_json(redis_connection.get(key).decode('utf-8'))
    return df

# Connection parameters for MySQL
mysql_connection_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
}

# MySQL queries
mysql_customer_query = 'SELECT * FROM customer;'
mysql_lineitem_query = """
SELECT
    L_ORDERKEY,
    SUM(L_QUANTITY) as SUM_QUANTITY
FROM
    lineitem
GROUP BY
    L_ORDERKEY
HAVING
    SUM(L_QUANTITY) > 300;
"""

# Fetching data from MySQL
df_customer = query_mysql(mysql_customer_query, mysql_connection_params)
df_lineitem = query_mysql(mysql_lineitem_query, mysql_connection_params)

# Connection parameters for Redis
redis_hostname = 'redis'
redis_port = 6379
redis_db_number = 0

# Fetching data from Redis
df_orders = get_redis_df('orders', redis_hostname, redis_port, redis_db_number)

# Joining and filtering dataframes
df_merged = df_customer.merge(df_orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
df_merged = df_merged.merge(df_lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Grouping and sorting the result
df_final = df_merged.groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE'], as_index=False)\
    .agg({'SUM_QUANTITY': 'sum'})\
    .sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])

# Output the result
df_final.to_csv('query_output.csv', index=False)
