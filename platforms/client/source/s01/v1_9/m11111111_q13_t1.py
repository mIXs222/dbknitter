from pymongo import MongoClient
import pandas as pd

# connect to the MongoDB server, database and respective collections
client = MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]
customer_coll = db["customer"]
orders_coll = db["orders"]

# get data from collections
cust_data = pd.DataFrame(list(customer_coll.find()))
orders_data = pd.DataFrame(list(orders_coll.find()))

# Perform left join
merged = pd.merge(cust_data, orders_data, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='left')

# Exclude rows with O_COMMENT containing 'pending' and 'deposits'
merged = merged[~merged['O_COMMENT'].str.contains('pending|deposits', na=False)]

c_orders = merged.groupby('C_CUSTKEY')['O_ORDERKEY'].count().reset_index(name='C_COUNT')

custdist = c_orders.groupby('C_COUNT').size().reset_index(name='CUSTDIST')

# Sort by CUSTDIST descending and C_COUNT descending
result = custdist.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False])

# Write to CSV
result.to_csv('query_output.csv', index=False)
