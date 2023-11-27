import pandas as pd
import direct_redis

# Initialize direct_redis.DirectRedis connection
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Redis does not store data in tables, but for the use-case let's assume 'get' function
# retrieves a pandas dataframe similar to a SQL table
nation = redis_connection.get('nation')
region = redis_connection.get('region')
supplier = redis_connection.get('supplier')
customer = redis_connection.get('customer')
orders = redis_connection.get('orders')
lineitem = redis_connection.get('lineitem')

# Join the dataframes based on the query conditions
asia_region = region[region['R_NAME'] == 'ASIA']
nation_asia = nation[nation['N_REGIONKEY'].isin(asia_region['R_REGIONKEY'])]
supplier_asia = supplier[supplier['S_NATIONKEY'].isin(nation_asia['N_NATIONKEY'])]
customer_asia = customer[customer['C_NATIONKEY'].isin(nation_asia['N_NATIONKEY'])]
orders_asia = orders[
    (orders['O_CUSTKEY'].isin(customer_asia['C_CUSTKEY']))
    & (orders['O_ORDERDATE'] >= '1990-01-01')
    & (orders['O_ORDERDATE'] < '1995-01-01')
]
lineitem_asia = lineitem[
    (lineitem['L_SUPPKEY'].isin(supplier_asia['S_SUPPKEY']))
    & (lineitem['L_ORDERKEY'].isin(orders_asia['O_ORDERKEY']))
]

# Calculate revenue volume for all qualifying lineitems
lineitem_asia['REVENUE'] = lineitem_asia['L_EXTENDEDPRICE'] * (1 - lineitem_asia['L_DISCOUNT'])
lineitem_asia = lineitem_asia.merge(nation_asia, how='left', left_on='L_SUPPKEY', right_on='N_NATIONKEY')

# Group by nation and sum revenue
result = lineitem_asia.groupby(['N_NAME'], as_index=False)['REVENUE'].sum()

# Sort by revenue in descending order
result_sorted = result.sort_values(by=['REVENUE'], ascending=[False])

# Write output to CSV
result_sorted.to_csv('query_output.csv', index=False)
