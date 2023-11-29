import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Get lineitem data from MySQL where shipdate is within the specified range.
sql_query = """
SELECT
    L_SUPPKEY,
    L_EXTENDEDPRICE,
    L_DISCOUNT
FROM
    lineitem
WHERE
    L_SHIPDATE >= '1996-01-01' AND L_SHIPDATE < '1996-04-01'
"""
lineitem_df = pd.read_sql(sql_query, con=mysql_conn)

# Calculate the total revenue per supplier
lineitem_df['TOTAL_REVENUE'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])
supplier_revenue = lineitem_df.groupby('L_SUPPKEY', as_index=False)['TOTAL_REVENUE'].sum()

# Get the maximum revenue value
max_revenue = supplier_revenue['TOTAL_REVENUE'].max()

# Find the suppliers with the maximum revenue
top_suppliers = supplier_revenue[supplier_revenue['TOTAL_REVENUE'] == max_revenue]

# Get supplier data from Redis
supplier_df = pd.read_msgpack(redis_conn.get('supplier'))

# Merge supplier data with top suppliers
output_df = pd.merge(
    top_suppliers,
    supplier_df,
    how='inner',
    left_on='L_SUPPKEY',
    right_on='S_SUPPKEY'
)

# Select and rename the output columns
output_df = output_df[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE']]
output_df.rename(columns={'S_SUPPKEY': 'S_SUPPKEY', 'S_NAME': 'S_NAME', 'S_ADDRESS': 'S_ADDRESS', 'S_PHONE': 'S_PHONE'}, inplace=True)

# Sort the output data by S_SUPPKEY
output_df.sort_values(by='S_SUPPKEY', inplace=True)

# Write the query output to a CSV file
output_df.to_csv('query_output.csv', index=False)

# Close the connections
mysql_conn.close()
redis_conn.close()
