import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Function to get data from MySQL
def get_mysql_data(connection_info, query):
    conn = pymysql.connect(host=connection_info['hostname'],
                           user=connection_info['username'],
                           password=connection_info['password'],
                           db=connection_info['database_name'])
    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()
    return df

# Function to get data from Redis as DataFrame
def get_redis_data(hostname, port, table_name):
    redis_client = DirectRedis(host=hostname, port=port)
    data = redis_client.get(table_name)  # Assuming data is already in DataFrame-ready format
    df = pd.read_json(data, orient='split')
    return df

# MySQL connection information
mysql_info = {
    'database_name': 'tpch',
    'username': 'root',
    'password': 'my-secret-pw',
    'hostname': 'mysql'
}

# Redis connection information
redis_info = {
    'database_name': '0',
    'port': 6379,
    'hostname': 'redis'
}

# SQL queries for MySQL
mysql_query_supplier = """
SELECT S_SUPPKEY, S_NAME
FROM supplier
JOIN nation ON supplier.S_NATIONKEY = nation.N_NATIONKEY
WHERE nation.N_NAME = 'CANADA';
"""

mysql_query_part = """
SELECT P_PARTKEY
FROM part
WHERE P_NAME LIKE '%forest%';
"""

# Get the data from MySQL
df_supplier = get_mysql_data(mysql_info, mysql_query_supplier)
df_part = get_mysql_data(mysql_info, mysql_query_part)

# Get the data from Redis
df_partsupp = get_redis_data(redis_info['hostname'], redis_info['port'], 'partsupp')
df_lineitem = get_redis_data(redis_info['hostname'], redis_info['port'], 'lineitem')

# Perform analysis to identify suppliers with excess of forest part
# Filter lineitem for the relevant time period and relevant part.
df_lineitem = df_lineitem[df_lineitem['L_SHIPDATE'].between('1994-01-01', '1995-01-01')]
df_lineitem = df_lineitem[df_lineitem['L_PARTKEY'].isin(df_part['P_PARTKEY'])]

# Calculate total quantity of forest parts shipped per supplier
supplier_forest_parts_shipped = df_lineitem.groupby('L_SUPPKEY').agg({'L_QUANTITY': 'sum'}).reset_index()
supplier_forest_parts_shipped.columns = ['PS_SUPPKEY', 'TOTAL_SHIPPED']

# Find parts supplier with more than 50% forest parts
df_partsupp['TOTAL'] = df_partsupp.groupby('PS_SUPPKEY')['PS_AVAILQTY'].transform('sum')
df_partsupp = df_partsupp[df_partsupp['PS_PARTKEY'].isin(df_part['P_PARTKEY'])]

supplier_excess_parts = df_partsupp[df_partsupp['PS_AVAILQTY'] > 0.5 * df_partsupp['TOTAL']]

# Further filter to suppliers with excess parts who have shipped those parts
supplier_with_excess = supplier_excess_parts.merge(supplier_forest_parts_shipped, on='PS_SUPPKEY')
result = supplier_with_excess.merge(df_supplier, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Select relevant columns and write to CSV
result[['S_SUPPKEY', 'S_NAME']].to_csv('query_output.csv', index=False)
