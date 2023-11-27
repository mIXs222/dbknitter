import pandas as pd
from direct_redis import DirectRedis

# Establish a connection to Redis
redis_hostname = 'redis'
redis_port = 6379
redis_db = 0
redis_client = DirectRedis(host=redis_hostname, port=redis_port, db=redis_db)

# Retrieve data from Redis
customer_df = pd.read_json(redis_client.get('customer'))
orders_df = pd.read_json(redis_client.get('orders'))
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Convert data types to proper formats
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])

# Filter by market segment and date
customer_orders = pd.merge(customer_df[customer_df['C_MKTSEGMENT'] == 'BUILDING'],
                           orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# Filter out orders that have been shipped
filtered_orders = customer_orders[customer_orders['O_ORDERDATE'] < '1995-03-15']

# Join with lineitem
result = pd.merge(filtered_orders, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Calculate revenue and shipping priority
result['REVENUE'] = result['L_EXTENDEDPRICE'] * (1 - result['L_DISCOUNT'])
result_grouped = result.groupby(by='O_ORDERKEY').agg({'REVENUE': 'sum', 'O_SHIPPRIORITY': 'first'})

# Sort by revenue in descending order
result_final = result_grouped.sort_values(by='REVENUE', ascending=False)

# Selecting necessary columns and renaming them
result_final = result_final[['O_SHIPPRIORITY', 'REVENUE']]

# Write the output to a CSV file
result_final.to_csv('query_output.csv', index=False)
