from pymongo import MongoClient
import pandas as pd

# Creating a client instance
client = MongoClient("mongodb://mongodb:27017/")

# Connecting to the tpch database
db = client["tpch"]

# Accessing the collections (tables)
supplier = db['supplier']
lineitem = db['lineitem']
orders = db['orders']
customer = db['customer']
nation = db['nation']

# Write your queries according to pymongo's syntax and join the result as per your needs
suppliers = supplier.find({})
lineitems = lineitem.find({})
orders_list = orders.find({})
customers = customer.find({})
nations = nation.find({})

# Transform the data to pandas dataframes
df_suppliers = pd.DataFrame(list(suppliers))
df_lineitems = pd.DataFrame(list(lineitems))
df_orders = pd.DataFrame(list(orders_list))
df_customers = pd.DataFrame(list(customers))
df_nations = pd.DataFrame(list(nations))

# Write the code to execute JOIN operation and aggregate functions similar to SQL query as per your requirement
# Then, Save the final result as CSV
# df_final.to_csv('query_output.csv')
