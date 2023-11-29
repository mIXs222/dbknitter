import pymysql
import pandas as pd
import direct_redis

# Connection to MySQL
conn_mysql = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Get supplier data
supplier_sql = "SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE FROM supplier"
supplier_df = pd.read_sql(supplier_sql, conn_mysql)
conn_mysql.close()

# Connection to Redis
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)
lineitem_df_bytes = redis_connection.get('lineitem')
lineitem_df = pd.read_msgpack(lineitem_df_bytes)  # Assuming the Redis data is msgpacked

# Convert shipdate to datetime and filter
lineitem_df['ship_date_converted'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
filtered_lineitem_df = lineitem_df[(lineitem_df['ship_date_converted'] >= '1996-01-01') & (lineitem_df['ship_date_converted'] <= '1996-04-01')]

# Calculate total revenue
filtered_lineitem_df['TOTAL_REVENUE'] = filtered_lineitem_df['L_EXTENDEDPRICE'] * (1 - filtered_lineitem_df['L_DISCOUNT'])

# Sum revenue by supplier and merge with supplier information
revenue_per_supplier = filtered_lineitem_df.groupby('L_SUPPKEY')['TOTAL_REVENUE'].sum().reset_index()
merged_df = supplier_df.merge(revenue_per_supplier, left_on='S_SUPPKEY', right_on='L_SUPPKEY')

# Find the maximum revenue
max_revenue = merged_df['TOTAL_REVENUE'].max()
top_suppliers_df = merged_df[merged_df['TOTAL_REVENUE'] == max_revenue]

# Sort and select the output columns
output_columns = ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE']
top_suppliers_df_sorted = top_suppliers_df.sort_values(by='S_SUPPKEY')[output_columns]

# Write to CSV
top_suppliers_df_sorted.to_csv('query_output.csv', index=False)
