import pymysql
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection function
def connect_mysql(host, database, user, password):
    connection = pymysql.connect(host=host, user=user, password=password, database=database)
    return connection

# Retrieve MySQL data
def get_mysql_data(conn, query):
    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
    return result

# Connect to MySQL
try:
    mysql_conn = connect_mysql('mysql', 'tpch', 'root', 'my-secret-pw')
    mysql_query = "SELECT N_NATIONKEY, N_NAME FROM nation"
    mysql_data = get_mysql_data(mysql_conn, mysql_query)
    df_nation = pd.DataFrame(mysql_data, columns=['N_NATIONKEY', 'N_NAME'])
finally:
    mysql_conn.close()

# Connect to Redis and retrieve data
r = DirectRedis(host='redis', port=6379, db=0, decode_responses=True)
df_customer = pd.read_json(r.get('customer'))
df_orders = pd.read_json(r.get('orders'))
df_lineitem = pd.read_json(r.get('lineitem'))

# Merge Redis dataframes
df_redis = df_customer.merge(df_orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
df_redis = df_redis.merge(df_lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Filter the orders and lineitem data
df_redis = df_redis[(df_redis['O_ORDERDATE'] >= '1993-10-01') & (df_redis['O_ORDERDATE'] < '1994-01-01') & (df_redis['L_RETURNFLAG'] == 'R')]

# Combine the MySQL and Redis dataframes
combined_df = df_redis.merge(df_nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Perform the query calculations and group by
combined_df['REVENUE'] = combined_df['L_EXTENDEDPRICE'] * (1 - combined_df['L_DISCOUNT'])
result_df = combined_df.groupby(['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_PHONE', 'N_NAME', 'C_ADDRESS', 'C_COMMENT'])['REVENUE'].sum().reset_index()

# Sort the result dataframe
result_df = result_df.sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[True, True, True, False])

# Save to csv
result_df.to_csv('query_output.csv', index=False)
