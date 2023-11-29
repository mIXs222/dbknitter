import pandas as pd
import direct_redis

# Connection details for Redis
redis_host = 'redis'
redis_port = 6379

# Connect to Redis using direct_redis
client = direct_redis.DirectRedis(host=redis_host, port=redis_port)

# Retrieve tables from Redis
nation = pd.DataFrame(eval(client.get('nation')))
customer = pd.DataFrame(eval(client.get('customer')))
orders = pd.DataFrame(eval(client.get('orders')))
lineitem = pd.DataFrame(eval(client.get('lineitem')))

# Filter the orders that are within the given date range
orders_filtered = orders[(orders['O_ORDERDATE'] >= '1993-10-01') & (orders['O_ORDERDATE'] < '1994-01-01')]

# Filter the lineitems for returned items
lineitem_returned = lineitem[lineitem['L_RETURNFLAG'] == 'R']

# Calculate revenue lost for lineitems
lineitem_returned['REVENUE_LOST'] = lineitem_returned['L_EXTENDEDPRICE'] * (1 - lineitem_returned['L_DISCOUNT'])

# Aggregate revenue lost of returns at the customer level
customer_revenue_lost = lineitem_returned \
    .groupby('L_ORDERKEY')['REVENUE_LOST'] \
    .sum() \
    .reset_index() \
    .merge(orders_filtered, left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Merge tables to get final result
final_result = customer_revenue_lost \
    .merge(customer, left_on='O_CUSTKEY', right_on='C_CUSTKEY') \
    .merge(nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Select required columns with correct order
final_result = final_result[[
    'C_CUSTKEY',
    'C_NAME',
    'REVENUE_LOST',
    'C_ACCTBAL',
    'N_NAME',
    'C_ADDRESS',
    'C_PHONE',
    'C_COMMENT'
]] \
    .sort_values(by=['REVENUE_LOST', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], 
                 ascending=[True, True, True, False])

# Write output to csv
final_result.to_csv('query_output.csv', index=False)
