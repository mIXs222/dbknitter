import pandas as pd
from direct_redis import DirectRedis

# Establish a connection to the Redis database
redis_db = DirectRedis(host='redis', port=6379, db=0)

# Read the data from Redis into Pandas DataFrame
customer_df = pd.read_json(redis_db.get('customer'))
orders_df = pd.read_json(redis_db.get('orders'))
lineitem_df = pd.read_json(redis_db.get('lineitem'))

# Group line items by order key and sum the quantities
lineitem_grouped = lineitem_df.groupby('L_ORDERKEY').agg({'L_QUANTITY': 'sum'}).reset_index()

# Filter for orders with total quantity greater than 300
orders_with_large_quantity = lineitem_grouped[lineitem_grouped['L_QUANTITY'] > 300]['L_ORDERKEY']

# Join orders with large quantity with orders table on order key
large_orders_df = orders_df[orders_df['O_ORDERKEY'].isin(orders_with_large_quantity)]

# Join the customers with the orders
customer_orders_df = pd.merge(large_orders_df, customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY', how='inner')

# Add the sum of quantities to the main dataframe
full_df = pd.merge(customer_orders_df, lineitem_grouped, left_on='O_ORDERKEY', right_on='L_ORDERKEY', how='inner')

# Select relevant columns and rename for clarity
result_df = full_df[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'L_QUANTITY']]
result_df = result_df.rename(
    columns={
        'C_NAME': 'Customer Name',
        'C_CUSTKEY': 'Customer Key',
        'O_ORDERKEY': 'Order Key',
        'O_ORDERDATE': 'Order Date',
        'O_TOTALPRICE': 'Total Price',
        'L_QUANTITY': 'Total Quantity'
    }
)

# Sort the results by Total Price in descending order and then by Order Date
result_df = result_df.sort_values(by=['Total Price', 'Order Date'], ascending=[False, True])

# Write the output to a CSV file
result_df.to_csv('query_output.csv', index=False)
