# query.py
import pandas as pd
import direct_redis

# Initialize DirectRedis connection
client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

def get_redis_dataframe(table_name):
    data_json = client.get(table_name)
    return pd.read_json(data_json, orient='records')

# Loading tables from Redis database
nation_df = get_redis_dataframe('nation')
part_df = get_redis_dataframe('part')
supplier_df = get_redis_dataframe('supplier')
partsupp_df = get_redis_dataframe('partsupp')
lineitem_df = get_redis_dataframe('lineitem')

# Specify the dim to be searched within the part names
specified_dim = "SpecifiedDim"  # Example: Replace with the actual dim to be searched

# Merging dataframes to gather necessary information
merged_df = lineitem_df.merge(part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')
merged_df = merged_df.merge(partsupp_df, on=['PS_PARTKEY', 'PS_SUPPKEY'])
merged_df = merged_df.merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
merged_df = merged_df.merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Filtering for parts with the specified dim in their names
filtered_df = merged_df[merged_df['P_NAME'].str.contains(specified_dim)]

# Calculating profit
filtered_df['PROFIT'] = (filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])) - (filtered_df['PS_SUPPLYCOST'] * filtered_df['L_QUANTITY'])

# Extracting the year from the order date
filtered_df['YEAR'] = pd.to_datetime(filtered_df['L_SHIPDATE']).dt.year

# Grouping the results by nation and year, calculating total profit
result_df = filtered_df.groupby(['N_NAME', 'YEAR'])['PROFIT'].sum().reset_index()

# Sorting the results as per the requirements
result_df.sort_values(by=['N_NAME', 'YEAR'], ascending=[True, False], inplace=True)

# Writing the result to a CSV file
result_df.to_csv('query_output.csv', index=False)
