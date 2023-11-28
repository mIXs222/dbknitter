import pandas as pd
import direct_redis

# Initialize a DirectRedis object
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Read tables from Redis
nation = pd.DataFrame(redis_connection.get('nation'))
part = pd.DataFrame(redis_connection.get('part'))
supplier = pd.DataFrame(redis_connection.get('supplier'))
partsupp = pd.DataFrame(redis_connection.get('partsupp'))
orders = pd.DataFrame(redis_connection.get('orders'))
lineitem = pd.DataFrame(redis_connection.get('lineitem'))

# Filter parts with 'dim' in their names
parts_with_dim = part[part['P_NAME'].str.contains('dim')]

# Compute profit for each line item
lineitem = lineitem.merge(parts_with_dim, left_on='L_PARTKEY', right_on='P_PARTKEY')
lineitem = lineitem.merge(partsupp, on=['L_PARTKEY', 'L_SUPPKEY'])
lineitem['profit'] = lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT']) - (lineitem['PS_SUPPLYCOST'] * lineitem['L_QUANTITY'])

# Join with orders to get the order date
lineitem_with_orders = lineitem.merge(orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Extract year from order date
lineitem_with_orders['year'] = pd.to_datetime(lineitem_with_orders['O_ORDERDATE']).dt.year

# Join with supplier and nation to associate line items with nations
lineitem_with_nations = lineitem_with_orders.merge(supplier[['S_SUPPKEY', 'S_NATIONKEY']], on='S_SUPPKEY')
lineitem_with_nations = lineitem_with_nations.merge(nation[['N_NATIONKEY', 'N_NAME']], left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Group by nation and year and calculate the profit distribution
profit_distribution = lineitem_with_nations.groupby(['N_NAME', 'year'])['profit'].sum().reset_index()

# Order the results by nation ascending and year descending
ordered_profit_distribution = profit_distribution.sort_values(by=['N_NAME', 'year'], ascending=[True, False])

# Save results to CSV file
ordered_profit_distribution.to_csv('query_output.csv', index=False)
