import pandas as pd
from direct_redis import DirectRedis

# Connect to Redis
redis = DirectRedis(host='redis', port=6379, db=0)

# Fetch data from Redis
df_customer = pd.read_json(redis.get('customer'))
df_orders = pd.read_json(redis.get('orders'))
df_lineitem = pd.read_json(redis.get('lineitem'))

# Convert relevant columns to numeric types to ensure proper aggregation operations
df_lineitem['L_QUANTITY'] = pd.to_numeric(df_lineitem['L_QUANTITY'])
df_orders['O_TOTALPRICE'] = pd.to_numeric(df_orders['O_TOTALPRICE'])

# Filter lineitem for SUM(L_QUANTITY) > 300
lineitem_filtered = df_lineitem.groupby('L_ORDERKEY', as_index=False).filter(lambda g: g['L_QUANTITY'].sum() > 300)

# Merge dataframes based on the given conditions
merged_df = df_customer.merge(df_orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = merged_df.merge(lineitem_filtered, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Perform grouping and aggregation based on the SQL query
result = merged_df.groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE'], as_index=False) \
                  .agg({'L_QUANTITY': 'sum'})

# Order the resultant data as per the SQL query
result = result.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])

# Select required columns to match the SQL query SELECT statement
result = result[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'L_QUANTITY']]

# Write to CSV
result.to_csv('query_output.csv', index=False)
