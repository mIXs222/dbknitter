import pandas as pd
from direct_redis import DirectRedis

# Connect to Redis
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Read data from Redis into Pandas DataFrames
df_customer = pd.read_json(redis_connection.get('customer'))
df_orders = pd.read_json(redis_connection.get('orders'))

# Exclude orders with comments containing 'pending' or 'deposits'
df_orders_filtered = df_orders[
    ~df_orders['O_COMMENT'].str.contains('pending|deposits', case=False, na=False)
]

# Perform left join to ensure all customers are included
df_joined = df_customer.merge(
    df_orders_filtered,
    how='left',
    left_on='C_CUSTKEY',
    right_on='O_CUSTKEY'
)

# Calculate order count per customer and create a subquery-like DataFrame
df_cust_order_count = df_joined.groupby('C_CUSTKEY').agg(
    C_COUNT=('O_ORDERKEY', 'count')
).reset_index()

# Calculate distribution of customers based on their count of orders
cust_dist_df = df_cust_order_count.groupby('C_COUNT').agg(
    CUSTDIST=('C_CUSTKEY', 'count')
).sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False]).reset_index()

# Write results to CSV file
cust_dist_df.to_csv('query_output.csv', index=False)
