import pandas as pd
from datetime import datetime
import direct_redis

# Connect to Redis
hostname = "redis"
port = 6379
database_name = 0
client = direct_redis.DirectRedis(host=hostname, port=port, db=database_name)

# Fetch tables from Redis
supplier_df = pd.read_json(client.get('supplier').decode('utf-8'))
lineitem_df = pd.read_json(client.get('lineitem').decode('utf-8'))

# Convert the dates in lineitem to datetime objects
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Filter business quarter
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 3, 31)
filtered_lineitem_df = lineitem_df[(lineitem_df['L_SHIPDATE'] >= start_date) & 
                                   (lineitem_df['L_SHIPDATE'] <= end_date)]

# Calculate revenue
filtered_lineitem_df['TOTAL_REVENUE'] = filtered_lineitem_df['L_EXTENDEDPRICE'] * (1 - filtered_lineitem_df['L_DISCOUNT'])

# Calculate revenue for each supplier
revenue0 = filtered_lineitem_df.groupby('L_SUPPKEY')['TOTAL_REVENUE'].sum().reset_index()
revenue0.columns = ['S_SUPPKEY', 'TOTAL_REVENUE']

# Merge revenue with supplier details
result = supplier_df.merge(revenue0, on='S_SUPPKEY')

# Identify the supplier with the maximum total revenue
max_revenue_supplier = result[result['TOTAL_REVENUE'] == result['TOTAL_REVENUE'].max()]

# Order by S_SUPPKEY
max_revenue_supplier = max_revenue_supplier.sort_values('S_SUPPKEY')

# Write to CSV
max_revenue_supplier.to_csv('query_output.csv', index=False)
