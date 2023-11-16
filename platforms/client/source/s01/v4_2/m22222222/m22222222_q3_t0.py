import pandas as pd
import redis
import json

# Connect to Redis
r = redis.StrictRedis(host='redis', port=6379, db=0)

# Read tables from Redis as DataFrames
customer_str = r.get('customer')
orders_str = r.get('orders')
lineitem_str = r.get('lineitem')

# Convert JSON strings to pandas dataframes
customer = pd.read_json(customer_str, orient='records')
orders = pd.read_json(orders_str, orient='records')
lineitem = pd.read_json(lineitem_str, orient='records')

# Perform operations equivalent to SQL query
result = (
    customer.query("C_MKTSEGMENT == 'BUILDING'")
    .merge(
        orders.query("O_ORDERDATE < '1995-03-15'")
        .merge(
            lineitem.query("L_SHIPDATE > '1995-03-15'"),
            left_on='O_ORDERKEY',
            right_on='L_ORDERKEY',
            how='inner'
        ),
        left_on='C_CUSTKEY',
        right_on='O_CUSTKEY',
        how='inner'
    )
    .assign(REVENUE=lambda x: x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT']))
    .groupby(['L_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'], as_index=False)
    ['REVENUE'].sum()
    .sort_values(['REVENUE', 'O_ORDERDATE'], ascending=[False, True])
)

# Write result to CSV file
result.to_csv('query_output.csv', index=False)
