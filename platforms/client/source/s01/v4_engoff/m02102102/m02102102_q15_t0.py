import pymysql
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Execute the MySQL query
mysql_query = """
SELECT S_SUPPKEY, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS revenue
FROM supplier
JOIN lineitem ON S_SUPPKEY = L_SUPPKEY
WHERE L_SHIPDATE >= '1996-01-01' AND L_SHIPDATE < '1996-04-01'
GROUP BY S_SUPPKEY
"""
mysql_cursor.execute(mysql_query)
supplier_revenue = mysql_cursor.fetchall()

# Create a DataFrame for supplier data
df_supplier_revenue = pd.DataFrame(supplier_revenue, columns=['S_SUPPKEY', 'revenue'])

# Close the MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Redis connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Get lineitem DataFrame from Redis
df_lineitem = redis_conn.get('lineitem')

# Convert the combined data into a DataFrame and sort by revenue descending, then by S_SUPPKEY ascending
df_lineitem['revenue'] = df_lineitem['L_EXTENDEDPRICE'] * (1 - df_lineitem['L_DISCOUNT'])
df_lineitem_filtered = df_lineitem[(df_lineitem['L_SHIPDATE'] >= '1996-01-01') & (df_lineitem['L_SHIPDATE'] < '1996-04-01')]
df_revenue_by_supplier = df_lineitem_filtered.groupby('L_SUPPKEY')['revenue'].sum().reset_index()

# Merge data from both sources
df_merged = pd.merge(df_supplier_revenue, df_revenue_by_supplier, on='S_SUPPKEY', how='inner')
df_merged['total_revenue'] = df_merged['revenue_x'] + df_merged['revenue_y']

# Find the maximum revenue and filter the DataFrame
max_revenue = df_merged['total_revenue'].max()
df_top_suppliers = df_merged[df_merged['total_revenue'] == max_revenue]

# Sort by S_SUPPKEY and keep only required columns
df_top_suppliers_sorted = df_top_suppliers.sort_values('S_SUPPKEY')[['S_SUPPKEY', 'total_revenue']]

# Write the result to a CSV file
df_top_suppliers_sorted.to_csv('query_output.csv', index=False)
