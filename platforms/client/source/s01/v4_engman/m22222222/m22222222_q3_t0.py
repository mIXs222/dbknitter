import pandas as pd
from direct_redis import DirectRedis

# Connect to Redis
redis = DirectRedis(host='redis', port=6379, db=0)

# Load the data from Redis
# Assuming the Redis stores Pandas DataFrames encoded as strings.
df_customer = pd.read_json(redis.get('customer'))
df_orders = pd.read_json(redis.get('orders'))
df_lineitem = pd.read_json(redis.get('lineitem'))

# Preprocess the date columns
df_orders['O_ORDERDATE'] = pd.to_datetime(df_orders['O_ORDERDATE'])
df_lineitem['L_SHIPDATE'] = pd.to_datetime(df_lineitem['L_SHIPDATE'])

# Perform the query
result_df = (
    df_orders.merge(df_customer, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
    .merge(df_lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    [lambda x: (x['C_MKTSEGMENT'] == 'BUILDING') & 
               (x['O_ORDERDATE'] < pd.Timestamp('1995-03-05')) & 
               (x['L_SHIPDATE'] > pd.Timestamp('1995-03-15'))]
)

# Calculate the revenue
result_df['REVENUE'] = result_df['L_EXTENDEDPRICE'] * (1 - result_df['L_DISCOUNT'])

# Group by relevant fields
output_df = (
    result_df.groupby(['O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'])
    .agg({'REVENUE': 'sum'})
    .reset_index()
    .sort_values('REVENUE', ascending=False)
)

# Select the required columns
output_df = output_df[['O_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY']]

# Write the output
output_df.to_csv('query_output.csv', index=False)
