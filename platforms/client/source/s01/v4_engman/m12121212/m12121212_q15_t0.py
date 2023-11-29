# Python code (top_supplier_query.py)

import pandas as pd
import direct_redis

# Connect to Redis
redis_host = "redis"
redis_port = 6379
redis_db = 0
redis_client = direct_redis.DirectRedis(host=redis_host, port=redis_port, db=redis_db)

# Read data frames from Redis
df_supplier = pd.read_json(redis_client.get('supplier'))
df_lineitem = pd.read_json(redis_client.get('lineitem'))

# Filter line items for the given date range
df_lineitem_filtered = df_lineitem[
    (df_lineitem['L_SHIPDATE'] >= '1996-01-01') &
    (df_lineitem['L_SHIPDATE'] < '1996-04-01')
]

# Calculate revenue
df_lineitem_filtered['REVENUE'] = df_lineitem_filtered['L_EXTENDEDPRICE'] * (1 - df_lineitem_filtered['L_DISCOUNT'])

# Sum revenue by supplier, sorted by supplier key
revenue_per_supplier = df_lineitem_filtered.groupby('L_SUPPKEY')['REVENUE'].sum().reset_index()

# Get the maximum revenue figure
max_revenue = revenue_per_supplier['REVENUE'].max()

# Find suppliers that match the maximum revenue figure
top_suppliers = revenue_per_supplier[revenue_per_supplier['REVENUE'] == max_revenue].sort_values('L_SUPPKEY')

# Merge top suppliers information with supplier details
top_suppliers_details = top_suppliers.merge(df_supplier, left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Select the required output columns
output_columns = ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'REVENUE']
top_suppliers_final = top_suppliers_details[output_columns]
top_suppliers_final = top_suppliers_final.rename(columns={'REVENUE': 'TOTAL_REVENUE'})

# Write the result to the file
top_suppliers_final.to_csv('query_output.csv', index=False)
