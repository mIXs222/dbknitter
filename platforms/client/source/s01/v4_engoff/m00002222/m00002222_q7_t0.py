import pymysql
import pandas as pd
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connection to Redis is a placeholder as direct_redis must be properly defined or installed
# Assuming that direct_redis.DirectRedis works similarly to redis.Redis client

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Query MySQL for nation and supplier data
nation_query = """
    SELECT *
    FROM nation
    WHERE N_NAME IN ('INDIA', 'JAPAN')
"""
supplier_query = "SELECT * FROM supplier"

with mysql_conn.cursor() as cursor:
    cursor.execute(nation_query)
    nations = pd.DataFrame(cursor.fetchall(), columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])

    cursor.execute(supplier_query)
    suppliers = pd.DataFrame(cursor.fetchall(), columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])

# Get Redis data for customer, orders, and lineitem using direct_redis and pandas
customers = pd.read_json(redis_conn.get('customer'))
orders = pd.read_json(redis_conn.get('orders'))
lineitems = pd.read_json(redis_conn.get('lineitem'))

# Close MySQL Connection
mysql_conn.close()

# Combine data to execute the complex query
# Select the lineitems for the year 1995 and 1996
lineitems['L_SHIPDATE'] = pd.to_datetime(lineitems['L_SHIPDATE'])
lineitems_filtered = lineitems[(lineitems['L_SHIPDATE'].dt.year == 1995) | (lineitems['L_SHIPDATE'].dt.year == 1996)]

# Merge tables on their keys
merged_data = pd.merge(lineitems_filtered, orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merged_data = pd.merge(merged_data, customers, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
merged_data = pd.merge(merged_data, suppliers, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
merged_data = pd.merge(merged_data, nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Filter the merged data based on the criteria
filtered_data = merged_data[
    ((merged_data['N_NAME'] == 'INDIA') & (merged_data['C_NATIONKEY'] != merged_data['N_NATIONKEY'])) |
    ((merged_data['N_NAME'] == 'JAPAN') & (merged_data['C_NATIONKEY'] != merged_data['N_NATIONKEY']))
]

# Calculate the revenue
filtered_data['REVENUE'] = filtered_data['L_EXTENDEDPRICE'] * (1 - filtered_data['L_DISCOUNT'])

# Select the relevant columns and summarize the information
result = filtered_data.groupby(['N_NAME', 'C_NATIONKEY', filtered_data['L_SHIPDATE'].dt.year])['REVENUE'].sum().reset_index()

# Rename columns to match the query
result.rename(columns={'N_NAME': 'SUPPLIER_NATION', 'C_NATIONKEY': 'CUSTOMER_NATION', 'L_SHIPDATE': 'YEAR'}, inplace=True)

# Order by Supplier nation, Customer nation, and year
result.sort_values(by=['SUPPLIER_NATION', 'CUSTOMER_NATION', 'YEAR'], ascending=True, inplace=True)

# Write to CSV
result.to_csv('query_output.csv', index=False)
