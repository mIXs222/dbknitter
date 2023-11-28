import pandas as pd
import direct_redis

# Connect to the Redis database
redis_db = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Load tables from Redis
def get_dataframe_from_redis(table_name):
    df = pd.read_json(redis_db.get(table_name))
    return df

# Retrieve the data from Redis
customer_df = get_dataframe_from_redis('customer')
orders_df = get_dataframe_from_redis('orders')
lineitem_df = get_dataframe_from_redis('lineitem')

# Perform the subquery to get order keys with total quantity > 300
order_keys_with_large_quantity = (
    lineitem_df.groupby('L_ORDERKEY')
    .agg({'L_QUANTITY': 'sum'})
    .query('L_QUANTITY > 300')
    .index
)

# Merge orders and customers on customer key
orders_customers_df = orders_df.merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Merge the above result with line items on order key and select only orders with large quantities
result_df = (
    orders_customers_df[orders_customers_df['O_ORDERKEY'].isin(order_keys_with_large_quantity)]
    .merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    .groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE'])
    .agg({'L_QUANTITY': 'sum'})
    .reset_index()
    .sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])
)

# Write the result to a CSV file
result_df.to_csv('query_output.csv', index=False)
