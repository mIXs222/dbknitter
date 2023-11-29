import pandas as pd
from direct_redis import DirectRedis
import csv

# Connect to redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Fetch data from Redis
supplier_df = pd.read_json(redis_client.get('supplier'))
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Filter lineitem data for the given date range
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
filtered_lineitem_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= '1996-01-01') & 
    (lineitem_df['L_SHIPDATE'] <= '1996-04-01')
]

# Calculate revenue
filtered_lineitem_df['REVENUE'] = filtered_lineitem_df['L_EXTENDEDPRICE'] * (1 - filtered_lineitem_df['L_DISCOUNT'])

# Group by S_SUPPKEY and sum revenue to calculate total revenue
total_revenue_df = filtered_lineitem_df.groupby('L_SUPPKEY')['REVENUE'].sum().reset_index()

# Join supplier and total revenue data on S_SUPPKEY
result_df = pd.merge(supplier_df, total_revenue_df, left_on='S_SUPPKEY', right_on='L_SUPPKEY')

# Find the top supplier(s) based on total revenue
max_revenue = result_df['REVENUE'].max()
top_suppliers_df = result_df[result_df['REVENUE'] == max_revenue]

# Select the required output columns 
output_df = top_suppliers_df[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'REVENUE']]

# Rename the column for output
output_df = output_df.rename(columns={'REVENUE': 'TOTAL_REVENUE'})

# Sort by S_SUPPKEY
output_df = output_df.sort_values('S_SUPPKEY')

# Write to CSV
output_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
