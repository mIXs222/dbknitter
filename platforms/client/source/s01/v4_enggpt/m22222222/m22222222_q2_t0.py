import pandas as pd
from direct_redis import DirectRedis

# Connect to Redis database
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Utility function to get DataFrame from Redis
def get_df_from_redis(table_name):
    df_json = redis_client.get(table_name)
    if df_json:
        return pd.read_json(df_json)
    else:
        print(f"Data for table {table_name} is not available.")
        return pd.DataFrame()

# Load DataFrames from Redis
nation_df = get_df_from_redis('nation')
region_df = get_df_from_redis('region')
part_df = get_df_from_redis('part')
supplier_df = get_df_from_redis('supplier')
partsupp_df = get_df_from_redis('partsupp')

# Merge and filter the DataFrames as per the given query
eu_region = region_df[region_df['R_NAME'] == 'EUROPE']
eu_nations = nation_df[nation_df['N_REGIONKEY'].isin(eu_region['R_REGIONKEY'])]
eu_suppliers = supplier_df[supplier_df['S_NATIONKEY'].isin(eu_nations['N_NATIONKEY'])]
eu_parts = part_df[(part_df['P_SIZE'] == 15) & (part_df['P_TYPE'].str.contains('BRASS'))]

eu_supplied_parts = pd.merge(partsupp_df, eu_parts, how='inner', left_on='PS_PARTKEY', right_on='P_PARTKEY')
eu_suppliers_parts = pd.merge(eu_supplied_parts, eu_suppliers, how='inner', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
min_supply_cost = eu_suppliers_parts.groupby(['PS_PARTKEY', 'PS_SUPPKEY'])['PS_SUPPLYCOST'].transform('min')
eu_suppliers_parts = eu_suppliers_parts[eu_suppliers_parts['PS_SUPPLYCOST'] == min_supply_cost]

result = eu_suppliers_parts.merge(eu_nations, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Ordering the result
result = result.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

# Selecting the columns as per query
result = result[['S_ACCTBAL', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT', 'P_PARTKEY', 'P_MFGR', 'P_SIZE']]

# Write to CSV
result.to_csv('query_output.csv', index=False)
