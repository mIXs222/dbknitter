# top_supplier_query.py

import pandas as pd
import direct_redis

# Define the dates for filtering
start_date = pd.to_datetime("1996-01-01")
end_date = pd.to_datetime("1996-04-01")

# Create a connection to the Redis database
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Load supplier table into pandas DataFrame
supplier_df = pd.read_json(redis_conn.get('supplier'))

# Load lineitem table into pandas DataFrame
lineitem_df = pd.read_json(redis_conn.get('lineitem'))

# Convert relevant columns to datetime for comparison
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Filter lineitem for the given date range
filtered_lineitem = lineitem_df[(lineitem_df['L_SHIPDATE'] >= start_date) & (lineitem_df['L_SHIPDATE'] <= end_date)]

# Calculate the total revenue per supplier
filtered_lineitem['TOTAL_REVENUE'] = filtered_lineitem['L_EXTENDEDPRICE'] * (1 - filtered_lineitem['L_DISCOUNT'])

# Sum up revenue per supplier
revenue_per_supplier = filtered_lineitem.groupby('L_SUPPKEY')['TOTAL_REVENUE'].sum().reset_index()

# Find the suppliers with the top revenue
top_revenue = revenue_per_supplier['TOTAL_REVENUE'].max()
top_suppliers = revenue_per_supplier[revenue_per_supplier['TOTAL_REVENUE'] == top_revenue]

# Merge the top suppliers with the suppliers details
top_suppliers_details = pd.merge(top_suppliers, supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY', how='left')

# Sort by supplier number order
top_suppliers_details_sorted = top_suppliers_details.sort_values(by=['S_SUPPKEY'])

# Select necessary supplier columns
final_output = top_suppliers_details_sorted[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT', 'TOTAL_REVENUE']]

# Write DataFrame to CSV
final_output.to_csv('query_output.csv', index=False)
