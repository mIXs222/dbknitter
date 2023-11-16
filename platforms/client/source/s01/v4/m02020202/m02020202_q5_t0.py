# Python code to execute the query across databases and write the output to a csv file

import pymysql
import pandas as pd
from sqlalchemy import create_engine
import direct_redis

# Set up the MySQL connection
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch')

# Set up the direct_redis connection
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Define the query components specific to MySQL
mysql_query = """
SELECT
    nation.N_NATIONKEY,
    nation.N_NAME,
    orders.O_ORDERKEY,
    orders.O_CUSTKEY,
    orders.O_ORDERDATE
FROM
    nation INNER JOIN orders ON nation.N_NATIONKEY = orders.O_CUSTKEY
WHERE
    O_ORDERDATE >= '1990-01-01'
    AND O_ORDERDATE < '1995-01-01';
"""

# Define a function to load tables from redis and convert to DataFrames
def load_redis_table(table_name):
    table_data = redis_connection.get(table_name)
    if table_data:
        return pd.read_json(table_data)
    else:
        return None

# Load redis tables
redis_tables = {
    'region': load_redis_table('region'),
    'supplier': load_redis_table('supplier'),
    'customer': load_redis_table('customer'),
    'lineitem': load_redis_table('lineitem')
}

# Execute the MySQL query
with mysql_connection.cursor() as cursor:
    cursor.execute(mysql_query)
    mysql_data = cursor.fetchall()

# Convert MySQL data to DataFrame
mysql_df = pd.DataFrame(mysql_data, columns=['N_NATIONKEY', 'N_NAME', 'O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERDATE'])

# Combine the data from MySQL and redis to construct the desired DataFrame
combined_df = (
    mysql_df
    .merge(redis_tables['customer'], left_on='O_CUSTKEY', right_on='C_CUSTKEY')
    .merge(redis_tables['lineitem'], left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    .merge(redis_tables['supplier'], left_on='C_NATIONKEY', right_on='S_NATIONKEY')
    .merge(redis_tables['region'], left_on='N_REGIONKEY', right_on='R_REGIONKEY')
)

# Perform the calculation and grouping as per the original query
result_df = (
    combined_df[combined_df['R_NAME'] == 'ASIA']
    .groupby('N_NAME')
    .agg(REVENUE=pd.NamedAgg(column='L_EXTENDEDPRICE',
                             aggfunc=lambda x: (x * (1 - combined_df['L_DISCOUNT'])).sum()))
    .reset_index()
    .sort_values('REVENUE', ascending=False)
)

# Write the result to a CSV file
result_df.to_csv('query_output.csv', index=False)

# Close the connections
mysql_connection.close()
