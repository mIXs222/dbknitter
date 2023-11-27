import pandas as pd
from direct_redis import DirectRedis

# Connect to Redis
hostname = 'redis'
port = 6379
database_name = '0'
client = DirectRedis(host=hostname, port=port, db=database_name)

# Fetch the tables from Redis as Pandas DataFrames
customer_df = pd.DataFrame(client.get('customer'))
orders_df = pd.DataFrame(client.get('orders'))
lineitem_df = pd.DataFrame(client.get('lineitem'))

# Convert relevant columns to appropriate data types
customer_df['C_CUSTKEY'] = customer_df['C_CUSTKEY'].astype(int)
orders_df[['O_ORDERKEY', 'O_CUSTKEY']] = orders_df[['O_ORDERKEY', 'O_CUSTKEY']].astype(int)
lineitem_df[['L_ORDERKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT']] = lineitem_df[['L_ORDERKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT']].apply(pd.to_numeric)

# Filter data as per the query requirements
filtered_customer_df = customer_df[customer_df['C_MKTSEGMENT'] == 'BUILDING']
filtered_orders_df = orders_df[orders_df['O_ORDERDATE'] < '1995-03-15']
filtered_lineitem_df = lineitem_df[lineitem_df['L_SHIPDATE'] > '1995-03-15']

# Calculate the potential revenue
filtered_lineitem_df['Pot_Revenue'] = filtered_lineitem_df['L_EXTENDEDPRICE'] * (1 - filtered_lineitem_df['L_DISCOUNT'])

# Join the tables based on the foreign keys
merged_df = pd.merge(filtered_customer_df, filtered_orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
final_df = pd.merge(merged_df, filtered_lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Group by orderkey and calculate the sum of potential revenue for each
grouped_df = final_df.groupby('O_ORDERKEY').agg(
    O_ORDERPRIORITY=pd.NamedAgg(column='O_ORDERPRIORITY', aggfunc='first'),
    Total_Revenue=pd.NamedAgg(column='Pot_Revenue', aggfunc='sum')
).reset_index()

# Sort the data by Total_Revenue in descending order
final_output = grouped_df.sort_values('Total_Revenue', ascending=False)
final_output = final_output[['O_ORDERPRIORITY', 'Total_Revenue']]

# Write result to CSV
final_output.to_csv('query_output.csv', index=False)
