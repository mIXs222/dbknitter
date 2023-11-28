import pandas as pd
import direct_redis

# Establish a connection to the Redis database
redis_host = 'redis'
redis_port = 6379
redis_db = direct_redis.DirectRedis(host=redis_host, port=redis_port, db=0)

# Retrieve tables from the Redis database
df_nation = pd.read_json(redis_db.get('nation'), orient='records')
df_customer = pd.read_json(redis_db.get('customer'), orient='records')
df_orders = pd.read_json(redis_db.get('orders'), orient='records')
df_lineitem = pd.read_json(redis_db.get('lineitem'), orient='records')

# Filter data according to the specified conditions
filtered_orders = df_orders[
    (df_orders['O_ORDERDATE'] >= '1993-10-01') &
    (df_orders['O_ORDERDATE'] <= '1993-12-31')
]

filtered_lineitem = df_lineitem[df_lineitem['L_RETURNFLAG'] == 'R']

# Perform join operations to combine the data from different tables
merged_data = (
    df_customer
    .merge(filtered_orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
    .merge(filtered_lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    .merge(df_nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
)

# Calculate the revenue
merged_data['REVENUE'] = merged_data['L_EXTENDEDPRICE'] * (1 - merged_data['L_DISCOUNT'])

# Select required fields
result = merged_data[[
    'C_CUSTKEY', 'C_NAME', 'REVENUE', 'C_ACCTBAL',
    'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT'
]].copy()

# Group by the specified attributes, calculate sum of revenue
grouped_result = result.groupby([
    'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_ADDRESS', 'C_PHONE', 'N_NAME', 'C_COMMENT'
]).agg({'REVENUE': 'sum'}).reset_index()

# Order the results
final_result = grouped_result.sort_values(
    by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'],
    ascending=[True, True, True, False]
)

# Write the query output to a CSV file
final_result.to_csv('query_output.csv', index=False)
