import pandas as pd
import pymysql
from direct_redis import DirectRedis
from datetime import datetime

# Connect to MySQL and fetch suppliers table
mysql_conn = pymysql.connect(host='mysql', 
                             user='root', 
                             password='my-secret-pw', 
                             db='tpch')

with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT * FROM supplier")
    supplier_data = cursor.fetchall()

# Convert supplier data to DataFrame
supplier_df = pd.DataFrame(supplier_data, columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Fetch lineitem table
lineitem_df = pd.read_json(redis_conn.get('lineitem'))

# Join lineitem with supplier on S_SUPPKEY
joined_df = pd.merge(lineitem_df, supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Filter data between the dates
filtered_df = joined_df[(joined_df['L_SHIPDATE'] >= datetime(1996, 1, 1)) & (joined_df['L_SHIPDATE'] < datetime(1996, 4, 1))]

# Calculate revenue
filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])

# Sum revenue by supplier
revenue_per_supplier = filtered_df.groupby('S_SUPPKEY')['REVENUE'].sum().reset_index()

# Find the top supplier(s)
max_revenue = revenue_per_supplier['REVENUE'].max()
top_suppliers = revenue_per_supplier[revenue_per_supplier['REVENUE'] == max_revenue]

# Join with supplier information
top_suppliers = pd.merge(top_suppliers, supplier_df, on='S_SUPPKEY').sort_values(by=['S_SUPPKEY'])

# Select relevant columns
result = top_suppliers[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT']]

# Write to CSV
result.to_csv('query_output.csv', index=False)
