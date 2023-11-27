import pandas as pd
import direct_redis

# Establish a connection to Redis
dr = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Define the function to get DataFrame from Redis
def get_redis_df(table_name):
    return pd.read_json(dr.get(table_name), orient='records')

# Get tables from Redis
part_df = get_redis_df('part')
lineitem_df = get_redis_df('lineitem')

# Define functions to check for containers, brands, and size for each part type
def is_type_1(row):
    return (row['P_BRAND'] == 'Brand#12' and 
            row['P_CONTAINER'] in ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'] and
            1 <= row['P_SIZE'] <= 5)

def is_type_2(row):
    return (row['P_BRAND'] == 'Brand#23' and
            row['P_CONTAINER'] in ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'] and
            1 <= row['P_SIZE'] <= 10)

def is_type_3(row):
    return (row['P_BRAND'] == 'Brand#34' and
            row['P_CONTAINER'] in ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'] and
            1 <= row['P_SIZE'] <= 15)

# Filter parts based on types
part_df['TYPE'] = part_df.apply(lambda row: is_type_1(row) or is_type_2(row) or is_type_3(row), axis=1)
filtered_parts = part_df[part_df['TYPE']]

# Merge the lineitem and part dataframes on P_PARTKEY == L_PARTKEY
merged_df = pd.merge(filtered_parts, lineitem_df, left_on='P_PARTKEY', right_on='L_PARTKEY')

# Filter based on quantity and shipmode conditions
result_df = merged_df[((merged_df['L_QUANTITY'] >= 1) & (merged_df['L_QUANTITY'] <= 11) & (is_type_1(merged_df))) |
                      ((merged_df['L_QUANTITY'] >= 10) & (merged_df['L_QUANTITY'] <= 20) & (is_type_2(merged_df))) |
                      ((merged_df['L_QUANTITY'] >= 20) & (merged_df['L_QUANTITY'] <= 30) & (is_type_3(merged_df))) &
                      (merged_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
                      (merged_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')]

# Calculate the gross discounted revenue for the final output
result_df['DISCOUNT_PRICE'] = result_df['L_EXTENDEDPRICE'] * (1 - result_df['L_DISCOUNT'])

# Select the necessary columns for the output
final_df = result_df[['L_ORDERKEY', 'DISCOUNT_PRICE']]

# Write the final result to a CSV file
final_df.to_csv('query_output.csv', index=False)
