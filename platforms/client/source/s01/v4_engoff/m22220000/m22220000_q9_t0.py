import pymysql
import pandas as pd
import direct_redis

# MySQL connection parameters
mysql_params = {
    'database': 'tpch',
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
}

# Redis connection parameters
redis_params = {
    'host': 'redis',
    'port': 6379,
    'db': 0
}

# Connect to MySQL
mysql_connection = pymysql.connect(**mysql_params)
# Connect to Redis
redis_connection = direct_redis.DirectRedis(**redis_params)

# MySQL Query
mysql_query = """
SELECT
    l.L_PARTKEY, l.L_SUPPKEY,
    l.L_EXTENDEDPRICE, l.L_DISCOUNT, l.L_QUANTITY,
    l.L_SHIPDATE, s.S_NATIONKEY
FROM
    lineitem l
JOIN
    partsupp ps ON l.L_PARTKEY = ps.PS_PARTKEY AND l.L_SUPPKEY = ps.PS_SUPPKEY
JOIN
    supplier s ON l.L_SUPPKEY = s.S_SUPPKEY
"""

# Execute MySQL query
lineitem_supplier = pd.read_sql_query(mysql_query, mysql_connection)

# Get Redis data as Pandas DataFrames
nation_df = pd.read_json(redis_connection.get('nation'))
part_df = pd.read_json(redis_connection.get('part'))

# Close MySQL connection as it's no longer needed
mysql_connection.close()

# Merge Redis dataframes into the larger dataframe
df = pd.merge(lineitem_supplier, part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')
df = pd.merge(df, nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Calculate profit
df['profit'] = (df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT'])) - (df['ps_supplycost'] * df['L_QUANTITY'])

# Extract year from L_SHIPDATE
df['year'] = pd.to_datetime(df['L_SHIPDATE']).dt.year

# Group the data by nation and year, and calculate the total profit
result_df = df.groupby(['N_NAME', 'year'], as_index=False).agg({'profit': 'sum'})

# Sort the results - nations in ascending order, years in descending order
result_df.sort_values(by=['N_NAME', 'year'], ascending=[True, False], inplace=True)

# Write to CSV
result_df.to_csv('query_output.csv', index=False)
