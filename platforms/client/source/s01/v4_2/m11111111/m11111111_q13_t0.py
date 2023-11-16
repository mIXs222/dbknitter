from pymongo import MongoClient
import pandas as pd

# Create connection to the MongoDB
client = MongoClient('mongodb://mongodb:27017/')
db = client['tpch']

# Retrieve data from MongoDB
customer_data = pd.DataFrame(list(db.customer.find({}, {'_id': False})))
order_data = pd.DataFrame(list(db.orders.find({}, {'_id': False})))

# Merge the dataframes
merged_data = pd.merge(customer_data, order_data, how='left', left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# Filter rows based on the condition
filtered_data = merged_data[~merged_data['O_COMMENT'].str.contains('pending|deposits', na=False)]

# Group by 'C_CUSTKEY' and 'O_ORDERKEY'
grouped_data = filtered_data.groupby('C_CUSTKEY')['O_ORDERKEY'].agg('count').reset_index().rename(columns={'O_ORDERKEY':'C_COUNT'})

# Group by 'C_COUNT' and count 'C_CUSTKEY'
final_data = grouped_data.groupby('C_COUNT')['C_CUSTKEY'].agg('count').reset_index().rename(columns={'C_CUSTKEY':'CUSTDIST'})

# Sort values by 'CUSTDIST' and 'C_COUNT'
final_data.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False,False], inplace=True)

# Write to csv file
final_data.to_csv('query_output.csv', index=False)
