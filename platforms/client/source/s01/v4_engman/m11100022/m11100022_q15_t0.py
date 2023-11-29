import pandas as pd
import pymysql
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Query to retrieve suppliers from MySQL
supplier_query = """
SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE
FROM supplier
"""

suppliers_df = pd.read_sql(supplier_query, mysql_conn)
mysql_conn.close()

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get lineitem data from Redis as DataFrame
lineitem_df = pd.read_json(redis_conn.get('lineitem'))

# Filter lineitem data for the date range and calculate revenue
lineitem_filtered = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= '1996-01-01') & 
    (lineitem_df['L_SHIPDATE'] <= '1996-04-01')
]
lineitem_filtered['TOTAL_REVENUE'] = lineitem_filtered['L_EXTENDEDPRICE'] * (1 - lineitem_filtered['L_DISCOUNT'])

# Summarize revenue by supplier
revenue_by_supplier = lineitem_filtered.groupby('L_SUPPKEY')['TOTAL_REVENUE'].sum().reset_index()

# Merge supplier data with revenue
merged_data = pd.merge(suppliers_df, revenue_by_supplier, left_on='S_SUPPKEY', right_on='L_SUPPKEY')

# Find the maximum revenue
max_revenue = merged_data['TOTAL_REVENUE'].max()

# Filter suppliers with the maximum revenue
top_suppliers = merged_data[merged_data['TOTAL_REVENUE'] == max_revenue]

# Select the required columns
top_suppliers = top_suppliers[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE']]

# Write the result to a CSV
top_suppliers.to_csv('query_output.csv', index=False)
