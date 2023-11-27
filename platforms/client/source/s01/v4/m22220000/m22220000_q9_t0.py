import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Function to execute query on MySQL
def execute_mysql_query(connection, query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        data = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
    return pd.DataFrame(data, columns=columns)

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

# Query for MySQL
mysql_query = """
SELECT
    L_ORDERKEY,
    O_ORDERKEY,
    O_ORDERDATE,
    L_EXTENDEDPRICE,
    L_DISCOUNT,
    L_PARTKEY,
    L_SUPPKEY,
    PS_PARTKEY,
    PS_SUPPKEY,
    PS_SUPPLYCOST,
    PS_AVAILQTY
FROM
    orders,
    lineitem,
    partsupp
WHERE
    O_ORDERKEY = L_ORDERKEY
    AND L_SUPPKEY = PS_SUPPKEY
    AND L_PARTKEY = PS_PARTKEY
"""

# Execute MySQL query
mysql_data = execute_mysql_query(mysql_conn, mysql_query)

# Close MySQL connection
mysql_conn.close()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Get tables from Redis
nation_df = pd.read_json(redis_conn.get('nation').decode('utf-8'))
part_df = pd.read_json(redis_conn.get('part').decode('utf-8'))
supplier_df = pd.read_json(redis_conn.get('supplier').decode('utf-8'))

# Merge data from MySQL and Redis
merged_df = (mysql_data
    .merge(part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')
    .merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    .merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY'))

# Filter rows where P_NAME like '%dim%'
filtered_df = merged_df[merged_df['P_NAME'].str.contains('dim')]

# Calculate required fields
filtered_df['O_YEAR'] = pd.to_datetime(filtered_df['O_ORDERDATE']).dt.year
filtered_df['AMOUNT'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT']) - filtered_df['PS_SUPPLYCOST'] * filtered_df['L_QUANTITY']

# Group by NATION and O_YEAR and calculate SUM_PROFIT
result_df = (filtered_df.groupby(['N_NAME', 'O_YEAR'])
    .agg(SUM_PROFIT=('AMOUNT', 'sum'))
    .reset_index()
    .rename(columns={'N_NAME': 'NATION'})
    .sort_values(by=['NATION', 'O_YEAR'], ascending=[True, False]))

# Write to CSV
result_df.to_csv('query_output.csv', index=False)
