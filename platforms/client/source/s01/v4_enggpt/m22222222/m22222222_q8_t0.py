import pandas as pd
import direct_redis

# Connect to Redis
redis_host = "redis"
redis_port = 6379
r = direct_redis.DirectRedis(host=redis_host, port=redis_port)

# Retrieve data from Redis
def get_df(key):
    data = r.get(key)
    return pd.read_json(data)

df_nation = get_df('nation')
df_region = get_df('region')
df_part = get_df('part')
df_supplier = get_df('supplier')
df_orders = get_df('orders')
df_lineitem = get_df('lineitem')
df_customer = get_df('customer')

# Perform the necessary filtering and join operations
df_part = df_part[df_part['P_TYPE'] == 'SMALL PLATED COPPER']
df_region = df_region[df_region['R_NAME'] == 'ASIA']

# Combining dataframes based on keys
merged_df1 = pd.merge(df_supplier, df_nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
merged_df2 = pd.merge(merged_df1, df_region, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
merged_df3 = pd.merge(df_lineitem, df_part, left_on='L_PARTKEY', right_on='P_PARTKEY')
merged_df4 = pd.merge(merged_df3, merged_df2, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
merged_data = pd.merge(merged_df4, df_orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
final_merged_data = pd.merge(merged_data, df_customer, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Filter for specific nation 'INDIA' and years 1995 to 1996
final_merged_data = final_merged_data[
    (final_merged_data['N_NAME'] == 'INDIA') &
    (final_merged_data['O_ORDERDATE'] >= pd.Timestamp('1995-01-01')) &
    (final_merged_data['O_ORDERDATE'] <= pd.Timestamp('1996-12-31'))
]

# Calculate volume
final_merged_data['VOLUME'] = final_merged_data['L_EXTENDEDPRICE'] * (1 - final_merged_data['L_DISCOUNT'])

# Group by year and calculate market share
final_merged_data['YEAR'] = final_merged_data['O_ORDERDATE'].dt.year
volume_by_year = final_merged_data.groupby('YEAR')['VOLUME'].sum().reset_index()
total_volume_by_year = final_merged_data.groupby('YEAR')['VOLUME'].transform('sum')
final_merged_data['MARKET_SHARE'] = final_merged_data['VOLUME'] / total_volume_by_year

# Selecting required columns and sorting
market_share_by_year = final_merged_data[['YEAR', 'MARKET_SHARE']].drop_duplicates().sort_values('YEAR')

# Write output to csv
market_share_by_year.to_csv('query_output.csv', index=False)
