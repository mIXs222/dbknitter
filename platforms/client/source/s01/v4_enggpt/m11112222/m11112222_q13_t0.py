# code.py
import pandas as pd
from direct_redis import DirectRedis

# Establish a connection to the Redis database
r = DirectRedis(host='redis', port=6379, db=0)

# Read data from Redis into Pandas DataFrames
customer_df = pd.read_json(r.get('customer'))
orders_df = pd.read_json(r.get('orders'))

# Filter orders with comments containing 'pending' or 'deposits'
filtered_orders = orders_df[~orders_df['O_COMMENT'].str.contains('pending|deposits', case=False, na=False)]

# Perform left join on 'customer' and filtered 'orders'
merged_df = pd.merge(customer_df, filtered_orders, how='left', left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# Create subquery DataFrame with counts of orders per customer where the conditions are met
counts_per_customer = merged_df.groupby('C_CUSTKEY').size().reset_index(name='C_COUNT')

# Merge subquery with original customer DataFrame to include all customers
final_df = pd.merge(customer_df[['C_CUSTKEY']], counts_per_customer, how='left', on='C_CUSTKEY')

# Replace NaN with 0 for customers with no orders
final_df['C_COUNT'].fillna(0, inplace=True)

# Count distribution of customers based on their order counts
cust_dist_df = final_df.groupby('C_COUNT').size().reset_index(name='CUSTDIST')

# Sort the results
sorted_cust_dist_df = cust_dist_df.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False])

# Write result to CSV
sorted_cust_dist_df.to_csv('query_output.csv', index=False)
