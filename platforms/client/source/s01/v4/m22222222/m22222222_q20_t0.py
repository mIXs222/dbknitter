import direct_redis
import pandas as pd

# Establish connection to Redis database
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch data from Redis tables and load them as Pandas DataFrames
nation_df = pd.DataFrame(redis_connection.get('nation'))
part_df = pd.DataFrame(redis_connection.get('part'))
supplier_df = pd.DataFrame(redis_connection.get('supplier'))
partsupp_df = pd.DataFrame(redis_connection.get('partsupp'))
lineitem_df = pd.DataFrame(redis_connection.get('lineitem'))

# Filter 'part' DataFrame to get parts with names starting with 'forest'
forest_parts_df = part_df[part_df['P_NAME'].str.startswith('forest')]

# Filter 'partsupp' DataFrame for parts in 'forest_parts_df'
partsupp_subset_df = partsupp_df[partsupp_df['PS_PARTKEY'].isin(forest_parts_df['P_PARTKEY'])]

# Filter 'lineitem' DataFrame for the past year's data
lineitem_past_year_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= '1994-01-01') &
    (lineitem_df['L_SHIPDATE'] < '1995-01-01')
]

# Group by 'L_PARTKEY' and 'L_SUPPKEY' and calculate sum of 'L_QUANTITY'
quantity_by_partsupp = lineitem_past_year_df.groupby(['L_PARTKEY', 'L_SUPPKEY'])['L_QUANTITY'].sum().reset_index()

# Merge 'partsupp_subset_df' with the summed 'L_QUANTITY' from 'quantity_by_partsupp'
merged_partsupp_quantity = partsupp_subset_df.merge(
    quantity_by_partsupp,
    how='left',
    left_on=['PS_PARTKEY', 'PS_SUPPKEY'],
    right_on=['L_PARTKEY', 'L_SUPPKEY']
)

# Filter 'merged_partsupp_quantity' DataFrame to satisfy the quantity condition
partsupp_qualified_df = merged_partsupp_quantity[merged_partsupp_quantity['PS_AVAILQTY'] > 0.5 * merged_partsupp_quantity['L_QUANTITY']]

# Select only the 'PS_SUPPKEY'
supplier_keys = partsupp_qualified_df['PS_SUPPKEY']

# Filter 'supplier' DataFrame for suppliers in 'supplier_keys'
supplier_subset_df = supplier_df[supplier_df['S_SUPPKEY'].isin(supplier_keys)]

# Filter 'nation' DataFrame for Canada
canada_nation_df = nation_df[nation_df['N_NAME'] == 'CANADA']

# Merge 'supplier_subset_df' with 'canada_nation_df' on 'S_NATIONKEY' = 'N_NATIONKEY'
final_supplier_df = supplier_subset_df.merge(
    canada_nation_df,
    how='inner',
    left_on='S_NATIONKEY',
    right_on='N_NATIONKEY'
)

# Select only the desired columns and order the results
result = final_supplier_df[['S_NAME', 'S_ADDRESS']].sort_values('S_NAME')

# Write the results to a CSV file
result.to_csv('query_output.csv', index=False)
