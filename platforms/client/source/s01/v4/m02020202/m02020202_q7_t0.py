# Python code (query_data.py)
import pymysql
import pandas as pd
import direct_redis
from datetime import datetime

# MySQL connection setup
mysql_connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_connection.cursor()

# Redis connection setup
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch data from MySQL
try:
    mysql_cursor.execute("SELECT * FROM nation")
    nations_df = pd.DataFrame(mysql_cursor.fetchall(), columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])

    mysql_cursor.execute("SELECT * FROM orders WHERE O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'")
    orders_df = pd.DataFrame(mysql_cursor.fetchall(), columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT'])
finally:
    mysql_cursor.close()
    mysql_connection.close()

# Fetch data from Redis
supplier_df = pd.read_json(redis_connection.get('supplier'))
customer_df = pd.read_json(redis_connection.get('customer'))
lineitem_df = pd.read_json(redis_connection.get('lineitem'))

# Convert SHAPE_DATE to a datetime
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Pre-processing, join and filter tables
supplier_nation = nations_df.rename(columns={'N_NATIONKEY': 'S_NATIONKEY', 'N_NAME': 'SUPP_NATION'})
customer_nation = nations_df.rename(columns={'N_NATIONKEY': 'C_NATIONKEY', 'N_NAME': 'CUST_NATION'})

# Perform the join operations
merged_df = supplier_df.merge(supplier_nation, on='S_NATIONKEY') \
                        .merge(lineitem_df, on='S_SUPPKEY') \
                        .merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY') \
                        .merge(customer_df, on='C_CUSTKEY') \
                        .merge(customer_nation, on='C_NATIONKEY')

# Filter the results
filtered_df = merged_df[
    ((merged_df['SUPP_NATION'] == 'JAPAN') & (merged_df['CUST_NATION'] == 'INDIA')) |
    ((merged_df['SUPP_NATION'] == 'INDIA') & (merged_df['CUST_NATION'] == 'JAPAN'))
    & (merged_df['L_SHIPDATE'].dt.to_period('Y').astype(str).between('1995', '1996'))
]

# Select and calculate necessary columns
filtered_df['L_YEAR'] = filtered_df['L_SHIPDATE'].dt.year
filtered_df['VOLUME'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])

# Group by necessary fields
result_df = filtered_df.groupby(['SUPP_NATION', 'CUST_NATION', 'L_YEAR']).agg(REVENUE=('VOLUME', 'sum')).reset_index()

# Sort the results
result_df = result_df.sort_values(by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])

# Write to CSV
result_df.to_csv('query_output.csv', index=False)
