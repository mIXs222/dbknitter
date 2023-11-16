# query.py

import pymysql
import pandas as pd
import direct_redis

# MySQL connection parameters
mysql_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch'
}

# Redis connection parameters
redis_params = {
    'host': 'redis',
    'port': 6379,
    'db': 0
}

# Establish connection to MySQL
mysql_conn = pymysql.connect(**mysql_params)

# Read MySQL tables into DataFrames
part_df = pd.read_sql("SELECT * FROM part WHERE P_NAME LIKE '%dim%'", mysql_conn)
partsupp_df = pd.read_sql("SELECT * FROM partsupp", mysql_conn)
orders_df = pd.read_sql("SELECT * FROM orders", mysql_conn)
nation_df = pd.read_sql("SELECT * FROM nation", mysql_conn)

# Close MySQL connection
mysql_conn.close()

# Establish connection to Redis
redis_conn = direct_redis.DirectRedis(**redis_params)

# Read Redis tables into DataFrames
supplier_df = pd.read_json(redis_conn.get('supplier'))
lineitem_df = pd.read_json(redis_conn.get('lineitem'))

# Data preprocessing
# Convert O_ORDERDATE to datetime
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])

# Perform the join operation and calculation
merge1 = pd.merge(lineitem_df, supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
merge2 = pd.merge(merge1, partsupp_df, left_on=['L_PARTKEY', 'L_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY'])
merge3 = pd.merge(merge2, part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')
merge4 = pd.merge(merge3, orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merge5 = pd.merge(merge4, nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Select necessary columns
final_df = merge5[['N_NAME', 'O_ORDERDATE', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'PS_SUPPLYCOST', 'L_QUANTITY']]

# Calculate AMOUNT
final_df['AMOUNT'] = final_df['L_EXTENDEDPRICE'] * (1 - final_df['L_DISCOUNT']) - final_df['PS_SUPPLYCOST'] * final_df['L_QUANTITY']

# Group by N_NAME and O_YEAR
final_df['O_YEAR'] = final_df['O_ORDERDATE'].dt.year
result_df = final_df.groupby(['N_NAME', 'O_YEAR'])['AMOUNT'].sum().reset_index()
result_df = result_df.rename(columns={'N_NAME': 'NATION', 'AMOUNT': 'SUM_PROFIT'})

# Sort the results
result_df.sort_values(['NATION', 'O_YEAR'], ascending=[True, False], inplace=True)

# Write to CSV
result_df.to_csv('query_output.csv', index=False)
