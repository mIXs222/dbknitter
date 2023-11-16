import pandas as pd
from direct_redis import DirectRedis
import datetime

# Connect to Redis using DirectRedis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Helper to convert string to datetime
def convert_to_datetime(date_str):
    return datetime.datetime.strptime(date_str, '%Y-%m-%d')

# Helper to get and convert Redis data to Pandas DataFrame
def get_redis_table(table_name):
    data = redis_client.get(table_name)
    return pd.read_json(data) if data else None

# Get data from Redis
nation_df = get_redis_table('nation')
supplier_df = get_redis_table('supplier')
customer_df = get_redis_table('customer')
orders_df = get_redis_table('orders')
lineitem_df = get_redis_table('lineitem')

# Merge the dataframes to mimic the join operation in the SQL
merged_df = pd.merge(supplier_df, nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
merged_df.rename(columns={'N_NAME': 'SUPP_NATION'}, inplace=True)

merged_df = pd.merge(merged_df, lineitem_df, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
merged_df = pd.merge(merged_df, orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merged_df = pd.merge(merged_df, customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
merged_df = pd.merge(merged_df, nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY', suffixes=('', '_CUST'))

# Filter rows that match the condition
filtered_df = merged_df[
    ((merged_df['SUPP_NATION'] == 'JAPAN') & (merged_df['N_NAME'] == 'INDIA')) |
    ((merged_df['SUPP_NATION'] == 'INDIA') & (merged_df['N_NAME'] == 'JAPAN'))
]

# Filter rows based on the ship date
filtered_df['L_SHIPDATE'] = pd.to_datetime(filtered_df['L_SHIPDATE'])
filtered_df = filtered_df[
    (filtered_df['L_SHIPDATE'] >= convert_to_datetime('1995-01-01')) &
    (filtered_df['L_SHIPDATE'] <= convert_to_datetime('1996-12-31'))
]

# Perform the group by and aggregate operation
result_df = filtered_df.groupby(['SUPP_NATION', 'N_NAME', filtered_df['L_SHIPDATE'].dt.year.rename('L_YEAR')]).apply(
    lambda df: pd.Series({
        'REVENUE': (df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT'])).sum()
    })
).reset_index()

# Sort the results as specified in the SQL query
result_df.sort_values(by=['SUPP_NATION', 'N_NAME', 'L_YEAR'], inplace=True)

# Write the result to CSV
result_df.rename(columns={'N_NAME': 'CUST_NATION'}, inplace=True)
result_df.to_csv('query_output.csv', index=False)
