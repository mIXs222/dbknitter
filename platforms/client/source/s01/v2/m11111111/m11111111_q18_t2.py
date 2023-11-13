# dependencies: pandas, pymongo

import pandas as pd
from pymongo import MongoClient

# Create a connection
client = MongoClient("mongodb://localhost:27017/")
db = client['tpch']

# Read data from tables
customer = pd.DataFrame(list(db.customer.find({})))
orders = pd.DataFrame(list(db.orders.find({})))
lineitem = pd.DataFrame(list(db.lineitem.find({})))

# Remove the _id columns
customer = customer.drop('_id', axis=1)
orders = orders.drop('_id', axis=1)
lineitem = lineitem.drop('_id', axis=1)

# Merge tables
merged_df1 = pd.merge(customer, orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
final_df = pd.merge(merged_df1, lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Select L_ORDERKEY with SUM(L_QUANTITY) > 300
grouped_lineitem = lineitem.groupby('L_ORDERKEY')['L_QUANTITY'].sum().reset_index()
filtered_lineitem = grouped_lineitem[grouped_lineitem['L_QUANTITY'] > 300]['L_ORDERKEY'].tolist()

# Filter final dataframe
final_df = final_df[final_df['O_ORDERKEY'].isin(filtered_lineitem)]

# Groupby and sorting
final_df = final_df.groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE'])['L_QUANTITY'].sum().reset_index()
final_df = final_df.sort_values(['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])

# Write to csv
final_df.to_csv('query_output.csv', index=False)
