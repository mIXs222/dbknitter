import pymysql
import pandas as pd
import direct_redis
from sqlalchemy import create_engine

# Connection strings
mysql_connection_info = {
    'database': 'tpch',
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
}

redis_connection_info = {
    'host': 'redis',
    'port': 6379,
    'database': 0,
}

# Function to execute a query on MySQL and return a DataFrame
def get_mysql_data(query, connection_info):
    connection = pymysql.connect(
        host=connection_info['host'],
        user=connection_info['user'],
        password=connection_info['password'],
        database=connection_info['database']
    )
    try:
        return pd.read_sql(query, connection)
    finally:
        connection.close()

# Function to retrieve Redis data as a DataFrame
def get_redis_data(table_name):
    redis_client = direct_redis.DirectRedis(
        host=redis_connection_info['host'],
        port=redis_connection_info['port'],
        db=redis_connection_info['database']
    )
    return pd.read_msgpack(redis_client.get(table_name))

# Query parts
mysql_query = """
SELECT 
    supplier.S_SUPPKEY,
    nation.N_NATIONKEY, 
    nation.N_NAME,
    lineitem.L_EXTENDEDPRICE,
    lineitem.L_DISCOUNT,
    lineitem.L_QUANTITY,
    lineitem.L_PARTKEY,
    lineitem.L_ORDERKEY
FROM 
    supplier, 
    lineitem, 
    nation
WHERE 
    supplier.S_SUPPKEY = lineitem.L_SUPPKEY
    AND supplier.S_NATIONKEY = nation.N_NATIONKEY;
"""

# Execute queries and retrieve data
mysql_df = get_mysql_data(mysql_query, mysql_connection_info)
redis_nation = get_redis_data('nation')
redis_part = get_redis_data('part')
redis_partsupp = get_redis_data('partsupp')
redis_orders = get_redis_data('orders')

# Pre-merge filtering
redis_part = redis_part[redis_part['P_NAME'].str.contains('dim')]
mysql_df = mysql_df.merge(redis_part[['P_PARTKEY']], on='P_PARTKEY')
mysql_df = mysql_df.merge(redis_partsupp[['PS_PARTKEY', 'PS_SUPPKEY', 'PS_SUPPLYCOST']], 
                          left_on=['L_PARTKEY', 'S_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY'])
mysql_df = mysql_df.merge(redis_orders[['O_ORDERKEY', 'O_ORDERDATE']], 
                          left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Calculate derived columns
mysql_df['O_YEAR'] = pd.to_datetime(mysql_df['O_ORDERDATE']).dt.year
mysql_df['AMOUNT'] = mysql_df['L_EXTENDEDPRICE'] * (1 - mysql_df['L_DISCOUNT']) - mysql_df['PS_SUPPLYCOST'] * mysql_df['L_QUANTITY']

# Final Group By and Ordering
result_df = mysql_df[['N_NAME', 'O_YEAR', 'AMOUNT']].rename(columns={'N_NAME': 'NATION'}).groupby(['NATION', 'O_YEAR']).sum().reset_index()
result_df = result_df.sort_values(by=['NATION', 'O_YEAR'], ascending=[True, False])
result_df.columns = ['NATION', 'O_YEAR', 'SUM_PROFIT']

# Output to CSV
output_filename = 'query_output.csv'
result_df.to_csv(output_filename, index=False)
