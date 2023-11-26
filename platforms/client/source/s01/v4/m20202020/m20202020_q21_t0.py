import pymysql
import pandas as pd
import direct_redis
import csv

# Connect to MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Read relevant tables from MySQL
supplier_df = pd.read_sql('SELECT * FROM supplier', mysql_conn)
lineitem_df = pd.read_sql('SELECT * FROM lineitem', mysql_conn)

# Close MySQL connection
mysql_conn.close()

# Connect to Redis database
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)
nation_df = pd.read_json(redis_conn.get('nation').decode('utf-8'))
orders_df = pd.read_json(redis_conn.get('orders').decode('utf-8'))

# Merge the dataframes to create a combined dataframe
combined_df = supplier_df.merge(
    lineitem_df[lineitem_df['L_RECEIPTDATE'] > lineitem_df['L_COMMITDATE']],
    left_on='S_SUPPKEY', right_on='L_SUPPKEY'
).merge(
    orders_df[orders_df['O_ORDERSTATUS'] == 'F'], left_on='L_ORDERKEY', right_on='O_ORDERKEY'
).merge(
    nation_df[nation_df['N_NAME'] == 'SAUDI ARABIA'], left_on='S_NATIONKEY', right_on='N_NATIONKEY'
)

# groupby operation
result_df = combined_df.groupby('S_NAME').size().reset_index(name='NUMWAIT')

# Sort the result
result_df.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True], inplace=True)

# Write to CSV
result_df.to_csv('query_output.csv', index=False)
