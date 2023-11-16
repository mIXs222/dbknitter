import pandas as pd
from direct_redis import DirectRedis

# Initialize DirectRedis connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Fetch tables from Redis
nation = pd.DataFrame(eval(redis_conn.get('nation')))
region = pd.DataFrame(eval(redis_conn.get('region')))
part = pd.DataFrame(eval(redis_conn.get('part')))
supplier = pd.DataFrame(eval(redis_conn.get('supplier')))
partsupp = pd.DataFrame(eval(redis_conn.get('partsupp')))

# Merge the dataframes
merged_df = part.merge(partsupp, left_on='P_PARTKEY', right_on='PS_PARTKEY')
merged_df = merged_df.merge(supplier, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
merged_df = merged_df.merge(nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
merged_df = merged_df.merge(region, left_on='N_REGIONKEY', right_on='R_REGIONKEY')

# Apply where conditions
filtered_df = merged_df[
    (merged_df['P_SIZE'] == 15) &
    (merged_df['P_TYPE'].str.contains('BRASS')) &
    (merged_df['R_NAME'] == 'EUROPE')
]

# Get the minimum PS_SUPPLYCOST for the given conditions
min_supply_cost = filtered_df['PS_SUPPLYCOST'].min()

# Filter again with the minimum supply cost
final_df = filtered_df[filtered_df['PS_SUPPLYCOST'] == min_supply_cost]

# Select the required columns
final_df = final_df[
    ['S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY',
     'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT']
]

# Sort the dataframe
final_df = final_df.sort_values(
    by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'],
    ascending=[False, True, True, True]
)

# Write the output to a CSV file
final_df.to_csv('query_output.csv', index=False)
