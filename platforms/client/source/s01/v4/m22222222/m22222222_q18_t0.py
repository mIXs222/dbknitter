from direct_redis import DirectRedis
import pandas as pd

# Connect to the Redis database
r = DirectRedis(host='redis', port=6379, db=0)

# Load dataframes using the DirectRedis get method
customer_df = pd.read_json(r.get('customer'), orient='records')
orders_df = pd.read_json(r.get('orders'), orient='records')
lineitem_df = pd.read_json(r.get('lineitem'), orient='records')

# Filter out the line items with SUM(L_QUANTITY) > 300
filtered_lineitems = lineitem_df.groupby('L_ORDERKEY').filter(lambda x: x['L_QUANTITY'].sum() > 300)

# Merge customer, orders, and filtered line items on appropriate keys
result = pd.merge(customer_df, orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
result = pd.merge(result, filtered_lineitems, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Group by needed columns and calculate sum of L_QUANTITY
result = result.groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE'], as_index=False)['L_QUANTITY'].sum()

# Sort the final result by O_TOTALPRICE and O_ORDERDATE
result.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True], inplace=True)

# Output to CSV file
result.to_csv('query_output.csv', index=False)
