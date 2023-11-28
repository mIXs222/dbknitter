import pandas as pd
import pymysql
import direct_redis

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Defining the date range
start_date = '1996-01-01'
end_date = '1996-03-31'

# Load suppliers from MySQL
supplier_query = "SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE FROM supplier"
suppliers_df = pd.read_sql(supplier_query, mysql_connection)

# Connect to Redis
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Load lineitem DataFrame from Redis
lineitem_df = redis_connection.get('lineitem')

# Convert shipping dates to datetime and filter records
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
filtered_lineitems = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= start_date) & (lineitem_df['L_SHIPDATE'] <= end_date)
]

# Calculate revenue
filtered_lineitems['REVENUE'] = filtered_lineitems['L_EXTENDEDPRICE'] * (1 - filtered_lineitems['L_DISCOUNT'])
revenue_per_supplier = filtered_lineitems.groupby('L_SUPPKEY')['REVENUE'].sum().reset_index()

# Join suppliers with revenue
result_df = suppliers_df.merge(revenue_per_supplier, left_on='S_SUPPKEY', right_on='L_SUPPKEY')

# Identify the maximum revenue
max_revenue = result_df['REVENUE'].max()
result_df = result_df[result_df['REVENUE'] == max_revenue]

# Order result data
result_df = result_df.sort_values(by='S_SUPPKEY')

# Write the result to a CSV file
result_df.to_csv('query_output.csv', index=False)

# Close MySQL connection
mysql_connection.close()
