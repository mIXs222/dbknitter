import pymysql
import pandas as pd
from direct_redis import DirectRedis

def get_connection_mysql(db_name, user, passwd, host):
    connection = pymysql.connect(host=host,
                                 user=user,
                                 password=passwd,
                                 database=db_name,
                                 cursorclass=pymysql.cursors.Cursor)
    return connection

def read_table_mysql(connection, query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
    return df

def get_redis_connection(db, host, port):
    r = DirectRedis(host=host, port=port, db=db)
    return r

def read_dataframe_redis(connection, key):
    data = connection.get(key)
    df = pd.read_json(data)
    return df

# Connect to MySQL
conn_mysql = get_connection_mysql('tpch', 'root', 'my-secret-pw', 'mysql')
query_mysql = "SELECT P_PARTKEY, P_BRAND, P_CONTAINER FROM part WHERE P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG';"
df_parts = read_table_mysql(conn_mysql, query_mysql)
conn_mysql.close()

# Connect to Redis
conn_redis = get_redis_connection(0, 'redis', 6379)
df_lineitems = read_dataframe_redis(conn_redis, 'lineitem')

# Query logic combining MySQL and Redis data
df = pd.merge(df_lineitems, df_parts, left_on='L_PARTKEY', right_on='P_PARTKEY')
avg_quantity = df['L_QUANTITY'].mean()
df_small_qty = df[df['L_QUANTITY'] < 0.2 * avg_quantity]
annual_loss = df_small_qty.groupby(df_small_qty['L_SHIPDATE'].str[:4])['L_EXTENDEDPRICE'].sum().mean()

# Output to CSV
pd.DataFrame({'average_yearly_loss': [annual_loss]}).to_csv('query_output.csv', index=False)
