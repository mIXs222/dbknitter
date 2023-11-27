import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = client["tpch"]
nation_collection = mongodb["nation"]
orders_collection = mongodb["orders"]

# Convert MongoDB collections to Pandas DataFrames
df_nation = pd.DataFrame(list(nation_collection.find()))
df_orders = pd.DataFrame(list(orders_collection.find()))

# Connect to Redis
r = DirectRedis(host='redis', port=6379, db=0)

# Convert Redis data to Pandas DataFrames
df_supplier = pd.read_json(r.get('supplier'))
df_customer = pd.read_json(r.get('customer'))
df_lineitem = pd.read_json(r.get('lineitem'))

# Filter nations for INDIA and JAPAN
nation_filter = df_nation['N_NAME'].isin(['INDIA', 'JAPAN'])
df_filtered_nation = df_nation[nation_filter]

# Filter orders for the years 1995 and 1996
df_orders['O_ORDERDATE'] = pd.to_datetime(df_orders['O_ORDERDATE'])
orders_filter = df_orders['O_ORDERDATE'].dt.year.isin([1995, 1996])
df_filtered_orders = df_orders[orders_filter]

# Merge DataFrames to get supplier and customer nation
df_sup_nation = df_supplier.merge(df_filtered_nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY', how='inner')
df_cust_nation = df_customer.merge(df_filtered_nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY', how='inner')

# Filter lineitems for relevant order keys
lineitem_filter = df_lineitem['L_ORDERKEY'].isin(df_filtered_orders['O_ORDERKEY'])
df_filtered_lineitem = df_lineitem[lineitem_filter]

# Merge filtered lineitems with supplier and customer nation
df_result = df_filtered_lineitem.merge(df_sup_nation, on='S_SUPPKEY').merge(df_cust_nation, on='C_CUSTKEY')

# Calculate the revenue and year
df_result['YEAR'] = df_result['O_ORDERDATE'].dt.year
df_result['REVENUE'] = df_result['L_EXTENDEDPRICE'] * (1 - df_result['L_DISCOUNT'])

# Perform the final selection and ordering
df_result = df_result[(df_result['N_NAME_x'] != df_result['N_NAME_y']) & ((df_result['N_NAME_x'].isin(['INDIA', 'JAPAN'])) & (df_result['N_NAME_y'].isin(['INDIA', 'JAPAN'])))]
df_final = df_result[['N_NAME_x', 'N_NAME_y', 'YEAR', 'REVENUE']]
df_final = df_final.sort_values(by=['N_NAME_x', 'N_NAME_y', 'YEAR'])

# Write the result to CSV
df_final.to_csv('query_output.csv', index=False)
