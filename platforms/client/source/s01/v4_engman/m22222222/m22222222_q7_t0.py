import pandas as pd
import direct_redis

# Connection to Redis
hostname = 'redis'
port = 6379
database_name = 0
redis_client = direct_redis.DirectRedis(host=hostname, port=port, db=database_name)

# Fetch tables from Redis
nation_df = pd.DataFrame(redis_client.get('nation'))
supplier_df = pd.DataFrame(redis_client.get('supplier'))
customer_df = pd.DataFrame(redis_client.get('customer'))
lineitem_df = pd.DataFrame(redis_client.get('lineitem'))
orders_df = pd.DataFrame(redis_client.get('orders'))

# Filter for the given nations
nation_df = nation_df[nation_df.N_NAME.isin(['INDIA', 'JAPAN'])]

# Merge tables to get required data
merged_df = lineitem_df.merge(
    orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY'
).merge(
    customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY'
).merge(
    supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY'
).merge(
    nation_df.rename(columns={'N_NATIONKEY': 'S_NATIONKEY', 'N_NAME': 'SUPP_NATION'}), on='S_NATIONKEY'
).merge(
    nation_df.rename(columns={'N_NATIONKEY': 'C_NATIONKEY', 'N_NAME': 'CUST_NATION'}), on='C_NATIONKEY'
)

# Filter for the given date range
merged_df['L_YEAR'] = pd.to_datetime(merged_df['L_SHIPDATE']).dt.year
merged_df = merged_df[
    (merged_df.L_YEAR == 1995) | (merged_df.L_YEAR == 1996)
]

# Calculate gross discounted revenues
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Selecting relevant columns
result_df = merged_df[[
    'CUST_NATION', 'L_YEAR', 'REVENUE', 'SUPP_NATION'
]]

# Filtering cross-nation shipping
result_df = result_df[
    (result_df['SUPP_NATION'] != result_df['CUST_NATION']) &
    (result_df['SUPP_NATION'].isin(['INDIA', 'JAPAN'])) &
    (result_df['CUST_NATION'].isin(['INDIA', 'JAPAN']))
]

# Group by supplier nation, customer nation, and year to get the sum of revenues
result_df = result_df.groupby(
    ['CUST_NATION', 'L_YEAR', 'SUPP_NATION'],
    as_index=False
).agg({'REVENUE': 'sum'})

# Order by supplier nation, customer nation, and year
result_df = result_df.sort_values(
    by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR']
)

# Write to CSV
result_df.to_csv('query_output.csv', index=False)
