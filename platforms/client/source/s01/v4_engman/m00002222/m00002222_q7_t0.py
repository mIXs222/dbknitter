# query.py
import pymysql
import pandas as pd
import direct_redis
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch required data from MySQL
mysql_query = """
SELECT s.S_SUPPKEY, s.S_NATIONKEY, n.N_NAME 
FROM supplier s 
JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
WHERE n.N_NAME IN ('INDIA', 'JAPAN');
"""

with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    supplier_data = cursor.fetchall()

supplier_df = pd.DataFrame(supplier_data, columns=['S_SUPPKEY', 'S_NATIONKEY', 'N_NAME'])

# Fetch required data from Redis
customer_df = r.get('customer')
orders_df = r.get('orders')
lineitem_df = r.get('lineitem')

# Convert strings to DataFrames
customer_df = pd.read_json(customer_df)
orders_df = pd.read_json(orders_df)
lineitem_df = pd.read_json(lineitem_df)

# Filter and process data
india_japan_suppliers = supplier_df[supplier_df['N_NAME'].isin(['INDIA', 'JAPAN'])]
result = (
    lineitem_df
    .merge(india_japan_suppliers, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    .merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    .merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
)

result = result[
    result['N_NAME_x'] != result['N_NAME_y']
].copy()

result['L_YEAR'] = pd.to_datetime(result['L_SHIPDATE']).dt.year
result['REVENUE'] = result['L_EXTENDEDPRICE'] * (1 - result['L_DISCOUNT'])
result = result[
    (result['L_YEAR'] == 1995) | (result['L_YEAR'] == 1996)
]

# Group and order results
final_result = result.groupby(['N_NAME_y', 'L_YEAR', 'N_NAME_x'])['REVENUE'].sum().reset_index()
final_result = final_result.rename(columns={'N_NAME_y': 'CUST_NATION', 'N_NAME_x': 'SUPP_NATION'})
final_result = final_result.sort_values(by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])

# Define columns order
columns_order = ['CUST_NATION', 'L_YEAR', 'REVENUE', 'SUPP_NATION']
final_result = final_result[columns_order]

# Write to CSV
final_result.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

# Close connections
mysql_conn.close()
