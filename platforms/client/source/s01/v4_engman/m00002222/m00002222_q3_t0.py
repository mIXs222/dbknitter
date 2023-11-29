# shipping_priority_query.py

import pandas as pd
from direct_redis import DirectRedis

# DirectRedis connection
redis_hostname = 'redis'
redis_port = 6379
redis_db = 0
redis_client = DirectRedis(host=redis_hostname, port=redis_port, db=redis_db)

# Retrieve data using Redis DirectRedis client
def get_dataframe_from_redis(table_name):
    return pd.read_msgpack(redis_client.get(table_name))

# Retrieve tables from Redis
customer_df = get_dataframe_from_redis('customer')
orders_df = get_dataframe_from_redis('orders')
lineitem_df = get_dataframe_from_redis('lineitem')

# Query execution on DataFrames
result_df = pd.merge(orders_df, customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
result_df = pd.merge(result_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Calculation of revenue
result_df['REVENUE'] = result_df['L_EXTENDEDPRICE'] * (1 - result_df['L_DISCOUNT'])

# Filtering based on the conditions
filtered_df = result_df[
    (result_df['O_ORDERDATE'] < '1995-03-05') & 
    (result_df['L_SHIPDATE'] > '1995-03-15') & 
    (result_df['C_MKTSEGMENT'] == 'BUILDING')
]

# Selecting and sorting the final output
output_df = filtered_df[['O_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY']]
output_df = output_df.sort_values(by='REVENUE', ascending=False)

# Writing to CSV file
output_df.to_csv('query_output.csv', index=False)
