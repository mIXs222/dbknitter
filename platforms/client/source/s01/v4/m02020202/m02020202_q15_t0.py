import pandas as pd
import direct_redis

# Connect to Redis
hostname = 'redis'
port = 6379
db_name = 0
redis_client = direct_redis.DirectRedis(host=hostname, port=port, db=db_name)

# Load data from Redis
supplier_df = pd.read_json(redis_client.get('supplier'))
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Filter lineitem data
lineitem_filtered = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= '1996-01-01') &
    (lineitem_df['L_SHIPDATE'] < pd.to_datetime('1996-01-01') + pd.DateOffset(months=3))
]

# Calculate revenue
revenue0 = lineitem_filtered.groupby('L_SUPPKEY').apply(
    lambda x: (x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT'])).sum()).reset_index()
revenue0.columns = ['SUPPLIER_NO', 'TOTAL_REVENUE']

# Find max revenue
max_revenue = revenue0['TOTAL_REVENUE'].max()

# Filter suppliers with max revenue
max_revenue_suppliers = revenue0[revenue0['TOTAL_REVENUE'] == max_revenue]
result_df = supplier_df.merge(max_revenue_suppliers, left_on='S_SUPPKEY', right_on='SUPPLIER_NO')

# Select required columns
result_df = result_df[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE']]

# Sort and save to CSV
result_df.sort_values('S_SUPPKEY').to_csv('query_output.csv', index=False)
