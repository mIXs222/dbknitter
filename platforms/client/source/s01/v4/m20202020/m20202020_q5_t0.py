import pymysql
import pandas as pd
import direct_redis
import datetime

def execute_mysql_query(connection_info, query):
    conn = pymysql.connect(host=connection_info['hostname'],
                           user=connection_info['username'],
                           password=connection_info['password'],
                           db=connection_info['database_name'])
    try:
        df = pd.read_sql_query(query, conn)
    finally:
        conn.close()
    return df

def get_redis_dataframe(connection_info, table_name):
    dr = direct_redis.DirectRedis(host=connection_info['hostname'],
                                  port=connection_info['port'],
                                  db=connection_info['database_name'])
    df = dr.get(table_name)
    return df

mysql_conn_info = {
    "database_name": "tpch",
    "username": "root",
    "password": "my-secret-pw",
    "hostname": "mysql"
}

redis_conn_info = {
    "database_name": 0,
    "port": 6379,
    "hostname": "redis"
}

query_mysql_tables = '''
SELECT
    S_NATIONKEY,
    S_ACCTBAL,
    C_CUSTKEY,
    C_NATIONKEY,
    L_EXTENDEDPRICE,
    L_DISCOUNT,
    L_ORDERKEY,
    L_SUPPKEY
FROM
    customer,
    lineitem,
    supplier
WHERE
    C_CUSTKEY = L_ORDERKEY
    AND L_SUPPKEY = S_SUPPKEY
    AND C_NATIONKEY = S_NATIONKEY;
'''

df_mysql = execute_mysql_query(mysql_conn_info, query_mysql_tables)
df_nation = get_redis_dataframe(redis_conn_info, 'nation')
df_orders = get_redis_dataframe(redis_conn_info, 'orders')

df_joined = df_mysql.merge(df_nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
df_joined = df_joined.merge(df_orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
df_joined = df_joined[(df_joined['N_REGIONKEY'] == 2) & 
                      (df_joined['O_ORDERDATE'] >= datetime.date(1990, 1, 1)) & 
                      (df_joined['O_ORDERDATE'] < datetime.date(1995, 1, 1))]

df_result = df_joined.groupby('N_NAME').agg(
    REVENUE=pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc=lambda x: sum(x * (1 - df_joined.loc[x.index, 'L_DISCOUNT'])))
).reset_index()

df_result.sort_values('REVENUE', ascending=False, inplace=True)

df_result.to_csv('query_output.csv', index=False)
