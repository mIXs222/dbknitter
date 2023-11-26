# File name: multi_db_query.py

import pymysql
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# Function to establish MySQL connection
def connect_mysql(dbname, username, password, hostname):
    connection = pymysql.connect(host=hostname,
                                 user=username,
                                 password=password,
                                 database=dbname,
                                 cursorclass=pymysql.cursors.Cursor)
    return connection

# Function to fetch MySQL data as DataFrame
def fetch_mysql_data(connection, query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        data = cursor.fetchall()
        cols = [column[0] for column in cursor.description]
    return pd.DataFrame(list(data), columns=cols)

# Function to load data from Redis
def get_redis_data(redis_instance, table_name):
    dataframe = pd.read_msgpack(redis_instance.get(table_name))
    return dataframe

# Installing/importing DirectRedis
redis_instance = DirectRedis(host='redis', port=6379)

# Tables in Redis
nation_df = get_redis_data(redis_instance, 'nation')

# MySQL connection
mysql_connection = connect_mysql(dbname='tpch',
                                 username='root',
                                 password='my-secret-pw',
                                 hostname='mysql')

# MySQL queries
supplier_query = 'SELECT * FROM supplier;'
lineitem_query = 'SELECT * FROM lineitem;'
customer_query = 'SELECT * FROM customer;'
region_query = 'SELECT * FROM region;'
orders_query = '''
    SELECT * FROM orders
    WHERE O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'
'''

# Fetching data from MySQL into DataFrames
supplier_df = fetch_mysql_data(mysql_connection, supplier_query)
lineitem_df = fetch_mysql_data(mysql_connection, lineitem_query)
customer_df = fetch_mysql_data(mysql_connection, customer_query)
region_df = fetch_mysql_data(mysql_connection, region_query)
orders_df = fetch_mysql_data(mysql_connection, orders_query)

# Close MySQL connection
mysql_connection.close()

# Filtering and merging DataFrames
merged_df = (orders_df
    .merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
    .merge(nation_df.add_prefix('C_'), how='inner', left_on='C_NATIONKEY', right_on='C_N_NATIONKEY')
    .merge(region_df, how='inner', left_on='C_N_REGIONKEY', right_on='R_REGIONKEY')
    .merge(nation_df.add_prefix('S_'), how='inner', left_on='C_NATIONKEY', right_on='S_N_NATIONKEY')
    .merge(supplier_df, how='inner', left_on='S_SUPPKEY', right_on='S_SUPPKEY')
    .merge(lineitem_df, how='inner', on='L_ORDERKEY')
)

# Filtering for 'ASIA' region and 'SMALL PLATED COPPER' part type
final_df = merged_df.query("R_NAME == 'ASIA' and P_TYPE == 'SMALL PLATED COPPER'")

# Calculating VOLUME
final_df['VOLUME'] = final_df['L_EXTENDEDPRICE'] * (1 - final_df['L_DISCOUNT'])

# Group by O_YEAR and calculating MKT_SHARE
result_df = final_df['VOLUME'].groupby(final_df['O_ORDERDATE'].dt.year).sum().reset_index()
result_df.columns = ['O_YEAR', 'TOTAL_VOLUME']
result_df['MKT_SHARE'] = final_df['VOLUME'].where(final_df['S_N_NAME'] == 'INDIA', 0).groupby(final_df['O_ORDERDATE'].dt.year).sum().reset_index(drop=True) / result_df['TOTAL_VOLUME']

# Writing dataframe to file
result_df.to_csv('query_output.csv', index=False)
