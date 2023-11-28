# python_code.py
import pandas as pd
from direct_redis import DirectRedis

# Connect to Redis
hostname = 'redis'
port = 6379
redis_connection = DirectRedis(host=hostname, port=port)

# Read data frames from Redis
nation_df = pd.read_json(redis_connection.get('nation'))
part_df = pd.read_json(redis_connection.get('part'))
supplier_df = pd.read_json(redis_connection.get('supplier'))
partsupp_df = pd.read_json(redis_connection.get('partsupp'))
lineitem_df = pd.read_json(redis_connection.get('lineitem'))

# Apply filters
# Filter nation by 'CANADA'
canada_nationkey = nation_df[nation_df['N_NAME'] == 'CANADA']['N_NATIONKEY'].iloc[0]

# Filter suppliers by 'CANADA'
suppliers_in_canada_df = supplier_df[supplier_df['S_NATIONKEY'] == canada_nationkey]

# Filter parts that start with 'forest'
parts_with_forest_df = part_df[part_df['P_NAME'].str.startswith('forest')]

# Find partsupps with part keys of parts starting with 'forest'
partsupps_with_forest_parts_df = partsupp_df[partsupp_df['PS_PARTKEY'].isin(parts_with_forest_df['P_PARTKEY'])]

# Calculate 50% of sum of quantities for line items within ship date range
lineitem_filtered_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= '1994-01-01') &
    (lineitem_df['L_SHIPDATE'] <= '1995-01-01')
]

# Group by part and supplier keys and sum the quantities, then calculate 50%
lineitem_grouped = lineitem_filtered_df.groupby(['L_PARTKEY', 'L_SUPPKEY'])['L_QUANTITY'].sum() * 0.5
lineitem_threshold_df = lineitem_grouped.reset_index()

# Find supplier keys that are in partsupp, part starts with 'forest', and meet quantity threshold
valid_suppliers = partsupp_df[
    (partsupp_df['PS_PARTKEY'].isin(parts_with_forest_df['P_PARTKEY'])) & 
    (partsupp_df['PS_SUPPKEY'].isin(suppliers_in_canada_df['S_SUPPKEY'])) &
    (partsupp_df['PS_AVAILQTY'] >= lineitem_threshold_df['L_QUANTITY'])
]['PS_SUPPKEY'].unique()

# Filter final suppliers based on valid suppliers
final_suppliers_df = suppliers_in_canada_df[suppliers_in_canada_df['S_SUPPKEY'].isin(valid_suppliers)]

# Select required columns and sort by S_NAME
result_df = final_suppliers_df[['S_NAME', 'S_ADDRESS']].sort_values(by='S_NAME')

# Write results to CSV
result_df.to_csv('query_output.csv', index=False)
