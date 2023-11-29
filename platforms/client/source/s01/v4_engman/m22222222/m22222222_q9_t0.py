import pandas as pd
from direct_redis import DirectRedis

# Connection to Redis database
redis_host = 'redis'
redis_port = 6379

# Initialize the connection to Redis database
redis_client = DirectRedis(host=redis_host, port=redis_port, db=0)

def get_dataframe(table_name):
    table_data = redis_client.get(table_name)
    return pd.read_json(table_data)

# Retrieve data from Redis
nation_df = get_dataframe('nation')
part_df = get_dataframe('part')
supplier_df = get_dataframe('supplier')
partsupp_df = get_dataframe('partsupp')
lineitem_df = get_dataframe('lineitem')

# Filtering the parts by name containing a dim (assuming 'dim' is the part name piece we are filtering by)
part_df_filtered = part_df[part_df['P_NAME'].str.contains('dim')]

# Merging dataframes to gather all necessary information for the calculation
merged_df = lineitem_df \
    .merge(part_df_filtered, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY') \
    .merge(partsupp_df, how='inner', left_on=['L_PARTKEY', 'L_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY']) \
    .merge(supplier_df, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY') \
    .merge(nation_df, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Calculating the year from the order date
merged_df['year'] = pd.to_datetime(merged_df['L_SHIPDATE']).dt.year

# Calculate the profit for each line item
merged_df['profit'] = (merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])) - (merged_df['PS_SUPPLYCOST'] * merged_df['L_QUANTITY'])

# Grouping by Nation and Year to get total profit
result_df = merged_df.groupby(['N_NAME', 'year']) \
                    .agg({'profit': 'sum'}) \
                    .reset_index() \
                    .sort_values(['N_NAME', 'year'], ascending=[True, False])

# Write the result to query_output.csv file
result_df.to_csv('query_output.csv', index=False)
