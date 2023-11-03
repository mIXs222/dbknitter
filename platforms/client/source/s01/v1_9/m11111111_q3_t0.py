from pymongo import MongoClient
import pandas as pd
from datetime import datetime

# Establish database connection
client = MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]

# Load collections
customer_coll = db["customer"]
orders_coll = db["orders"]
lineitem_coll = db["lineitem"]

clients = customer_coll.find({ "C_MKTSEGMENT": 'BUILDING' })
clients_df = pd.DataFrame(list(clients))

approved_orders = orders_coll.find({ "O_ORDERDATE": { "$lt": datetime.strptime('1995-03-15', "%Y-%m-%d") }})
approved_orders_df = pd.DataFrame(list(approved_orders))

shipped_items = lineitem_coll.find({ "L_SHIPDATE": { "$gt": datetime.strptime('1995-03-15', "%Y-%m-%d") }}) 
shipped_items_df = pd.DataFrame(list(shipped_items))

merged_df1 = pd.merge(clients_df, approved_orders_df, left_on = 'C_CUSTKEY', right_on = 'O_CUSTKEY')
merged_df2 = pd.merge(merged_df1, shipped_items_df, left_on = 'O_ORDERKEY', right_on = 'L_ORDERKEY')

merged_df2['REVENUE'] = merged_df2['L_EXTENDEDPRICE'] * (1 - merged_df2['L_DISCOUNT'])
selected_df = merged_df2[['L_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY']]

grouped_df = selected_df.groupby(['L_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY']).sum().sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, False])
grouped_df.to_csv('query_output.csv')
