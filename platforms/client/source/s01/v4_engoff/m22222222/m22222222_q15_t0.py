import pandas as pd
import direct_redis

# Connect to Redis
hostname = 'redis'
port = 6379
r = direct_redis.DirectRedis(host=hostname, port=port, db=0)

# Read tables from Redis
supplier_df = pd.DataFrame.from_records(r.get('supplier'), columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])
lineitem_df = pd.DataFrame.from_records(r.get('lineitem'), columns=['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT'])

# Format dates as datetime
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Filter line items by date
start_date = '1996-01-01'
end_date = '1996-04-01'
filtered_lineitem = lineitem_df[(lineitem_df['L_SHIPDATE'] >= start_date) & (lineitem_df['L_SHIPDATE'] < end_date)]

# Calculate the total revenue contribution per supplier
filtered_lineitem['REVENUE'] = filtered_lineitem['L_EXTENDEDPRICE'] * (1 - filtered_lineitem['L_DISCOUNT'])
supplier_revenue = filtered_lineitem.groupby('L_SUPPKEY')['REVENUE'].sum().reset_index(name='TOTAL_REVENUE')

# Find the maximum revenue
max_revenue = supplier_revenue['TOTAL_REVENUE'].max()

# Identify top suppliers
top_suppliers = supplier_revenue[supplier_revenue['TOTAL_REVENUE'] == max_revenue]

# Merge with supplier details 
joined_data = pd.merge(top_suppliers, supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY', how='inner').sort_values('S_SUPPKEY')

# Output results
joined_data.to_csv('query_output.csv', index=False)
