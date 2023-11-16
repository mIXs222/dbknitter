from pymongo import MongoClient
import pandas as pd

# MongoDB connection
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Retrieve data from collections
customers = pd.DataFrame(list(db.customer.find({}, {'_id': 0})))
orders = pd.DataFrame(list(db.orders.find({}, {'_id': 0})))
lineitem = pd.DataFrame(list(db.lineitem.find({}, {'_id': 0})))

# Perform query equivalent to SQL one
# First, we select the relevant L_ORDERKEY from lineitem with the given condition.
relevant_l_orderkey = lineitem.groupby('L_ORDERKEY').filter(lambda x: x['L_QUANTITY'].sum() > 300).L_ORDERKEY.unique()

# Then, we join the dataframes to simulate the SQL join
df_merged = customers.merge(orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY').merge(lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Filter out the rows where O_ORDERKEY is not in the list of relevant_l_orderkey
df_filtered = df_merged[df_merged['O_ORDERKEY'].isin(relevant_l_orderkey)]

# Group by the customer name, custkey, orderkey, orderdate, and totalprice, and calculate the sum of L_QUANTITY
result = df_filtered.groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE']).agg({'L_QUANTITY': 'sum'}).reset_index()

# Sort the results as specified
result_sorted = result.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])

# Write out to csv file
result_sorted.to_csv('query_output.csv', index=False)
