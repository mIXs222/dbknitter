# potential_part_promotion.py

import pandas as pd
import direct_redis

# Initialize the connection to Redis database
redis_host = 'redis'
redis_port = 6379
redis_db = 0
r = direct_redis.DirectRedis(host=redis_host, port=redis_port, db=redis_db)

# Read the tables from Redis
nation_df = pd.read_json(r.get('nation'))
part_df = pd.read_json(r.get('part'))
supplier_df = pd.read_json(r.get('supplier'))
partsupp_df = pd.read_json(r.get('partsupp'))
lineitem_df = pd.read_json(r.get('lineitem'))

# Filter the tables based on conditions
# Start with nations involved, which is CANADA
canada_nationkey = nation_df[nation_df['N_NAME'].str.lower() == 'canada']['N_NATIONKEY'].iloc[0]

# Filter suppliers by Canada's nationkey
suppliers_in_canada_df = supplier_df[supplier_df['S_NATIONKEY'] == canada_nationkey]

# Filter parts that share certain naming conventions (assuming 'forest' in the name)
part_forest_df = part_df[part_df['P_NAME'].str.contains('forest', case=False)]

# Filter lineitems shipped between the given dates
lineitem_filtered_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= '1994-01-01') &
    (lineitem_df['L_SHIPDATE'] < '1995-01-01')
]

# Now aggregate to find parts like the 'forest part' that the supplier shipped
partsupp_forest_df = pd.merge(part_forest_df, partsupp_df, left_on='P_PARTKEY', right_on='PS_PARTKEY')

# Merge with suppliers in Canada
suppliers_forest_partsupp_df = pd.merge(suppliers_in_canada_df, partsupp_forest_df, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')

# Merge with filtered lineitems
final_df = pd.merge(lineitem_filtered_df, suppliers_forest_partsupp_df, on=['L_PARTKEY', 'L_SUPPKEY'])

# Group by supplier and part to find out the quantity
grouped_df = final_df.groupby(['S_SUPPKEY', 'L_PARTKEY'])['L_QUANTITY'].sum().reset_index()

# Find suppliers who meet the excess criteria
excess_suppliers_df = grouped_df[grouped_df['L_QUANTITY'] > grouped_df['L_QUANTITY'].transform(lambda x: x.median()) * 1.5]

# Get supplier details
excess_suppliers_details_df = pd.merge(excess_suppliers_df, suppliers_in_canada_df, left_on='S_SUPPKEY', right_on='S_SUPPKEY')

# Select the required columns and output to CSV
excess_suppliers_details_df[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'L_PARTKEY', 'L_QUANTITY']].to_csv('query_output.csv', index=False)
