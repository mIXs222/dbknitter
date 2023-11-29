import pymysql
import pandas as pd
from direct_redis import DirectRedis
import datetime

# Function to connect to mysql
def connect_to_mysql():
    connection = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 db='tpch')
    return connection

# Function to execute query on MySQL
def fetch_mysql_data(connection, part_name_like):
    query = """
        SELECT 
            s.S_NATIONKEY, 
            YEAR(l.L_SHIPDATE) AS year, 
            SUM((l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) - (ps.PS_SUPPLYCOST * l.L_QUANTITY)) AS profit
        FROM 
            lineitem l
            JOIN supplier s ON l.L_SUPPKEY = s.S_SUPPKEY
            JOIN partsupp ps ON l.L_SUPPKEY = ps.PS_SUPPKEY AND l.L_PARTKEY = ps.PS_PARTKEY
            JOIN part p ON p.P_PARTKEY = l.L_PARTKEY
        WHERE 
            p.P_NAME LIKE %s
        GROUP BY 
            s.S_NATIONKEY, year
        ORDER BY 
            s.S_NATIONKEY, year DESC
    """
    with connection.cursor() as cursor:
        cursor.execute(query, ('%' + part_name_like + '%',))
        result = cursor.fetchall()
    return result

# Function to convert redis data to dataframe
def fetch_redis_data(redis_conn, table_name):
    table_data = redis_conn.get(table_name)
    df = pd.read_json(table_data)
    return df

# Combine MySQL and Redis data
def combine_data(mysql_data, nation_df):
    df = pd.DataFrame(mysql_data, columns=['N_NATIONKEY', 'year', 'profit'])
    combined_df = pd.merge(df, nation_df, how='left', left_on='N_NATIONKEY', right_on='N_NATIONKEY')
    combined_df.sort_values(['N_NAME', 'year'], ascending=[True, False], inplace=True)
    return combined_df[['N_NAME', 'year', 'profit']]

# Specified dim in the part names
specified_dim = 'specified_dim'

# Connect to MySQL and Redis
mysql_conn = connect_to_mysql()
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Fetch data from MySQL
mysql_data = fetch_mysql_data(mysql_conn, specified_dim)

# Fetch data from Redis
nation_df = fetch_redis_data(redis_conn, 'nation')

# Combine data and create the output
output_df = combine_data(mysql_data, nation_df)

# Write to CSV
output_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
