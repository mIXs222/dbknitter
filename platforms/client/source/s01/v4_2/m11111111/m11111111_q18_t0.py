# install.py

from pymongo import MongoClient
import pandas as pd

# Establish connection with mongodb
client = MongoClient("mongodb://mongodb:27017/")
db = client['tpch']

# Fetch data from the tables
customer_data = list(db.customer.find({}, {'_id':0}))
orders_data = list(db.orders.find({}, {'_id':0}))
lineitem_data = list(db.lineitem.find({}, {'_id':0}))

# Convert the data into pandas dataframes
df_customer = pd.DataFrame(customer_data)
df_orders = pd.DataFrame(orders_data)
df_lineitem = pd.DataFrame(lineitem_data)

# Merge the dataframes
result = pd.merge(df_customer, df_orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
result = pd.merge(result, df_lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Filter the data according to the condition given
result = result.groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE']).filter(lambda x: x['L_QUANTITY'].sum() > 300)

# Sum the L_QUANTITY
result['L_QUANTITY'] = result.groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE'])['L_QUANTITY'].transform('sum')

# Remove duplicates
result = result.drop_duplicates()

# Sort the data
result = result.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])

# Write the output to the csv file
result.to_csv('query_output.csv', index=False)
