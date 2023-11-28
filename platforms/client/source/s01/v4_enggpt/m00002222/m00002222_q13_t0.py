import pandas as pd
from direct_redis import DirectRedis

# Connect with Redis Database
r = DirectRedis(host='redis', port=6379, db=0)

# Fetch data into dataframes
df_customer = pd.DataFrame(eval(r.get('customer')))
df_orders = pd.DataFrame(eval(r.get('orders')))

# Filter orders that do not contain 'pending' or 'deposits' in the comment
filtered_orders = df_orders[~df_orders['O_COMMENT'].str.contains('pending|deposits', case=False, na=False)]

# Perform a left join on customer key
df_merged = pd.merge(df_customer,
                     filtered_orders,
                     how='left',
                     left_on='C_CUSTKEY',
                     right_on='O_CUSTKEY')

# Make a group to calculate the count of orders for each customer
grouped = df_merged.groupby('C_CUSTKEY').agg(C_COUNT=('O_ORDERKEY', 'count')).reset_index()

# Calculate distribution of customers by the count of their orders
dist_of_customers = grouped['C_COUNT'].value_counts().reset_index()
dist_of_customers.columns = ['C_COUNT', 'CUSTDIST']

# Order the results
dist_of_customers = dist_of_customers.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False])

# Save to CSV
dist_of_customers.to_csv('query_output.csv', index=False)
