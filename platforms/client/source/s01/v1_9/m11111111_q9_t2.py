import pymongo
from pymongo import MongoClient
import pandas as pd
from pandas.io.json import json_normalize

# Install pymongo using pip
# pip install pymongo

client = MongoClient('mongodb://mongodb:27017/')
db = client.tpch

# Fetch data from MongoDB
nation = list(db.nation.find())
region = list(db.region.find())
part = list(db.part.find())
supplier = list(db.supplier.find())
partsupp = list(db.partsupp.find())
customer = list(db.customer.find())
orders = list(db.orders.find())
lineitem = list(db.lineitem.find())

# Convert into DataFrames
df_nation = pd.DataFrame(nation)
df_region = pd.DataFrame(region)
df_part = pd.DataFrame(part)
df_supplier = pd.DataFrame(supplier)
df_partsupp = pd.DataFrame(partsupp)
df_customer = pd.DataFrame(customer)
df_orders = pd.DataFrame(orders)
df_lineitem = pd.DataFrame(lineitem)

# Data processing and calculations
df = df_lineitem.merge(df_partsupp, left_on=['L_PARTKEY', 'L_SUPPKEY'], 
                        right_on=['PS_PARTKEY', 'PS_SUPPKEY'])

df = df[df['P_PART'].str.contains('dim')]

df = df.merge(df_supplier, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
df = df.merge(df_nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
df = df.merge(df_orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')

df['O_YEAR'] = pd.to_datetime(df['O_ORDERDATE']).dt.year
df['AMOUNT'] = df['L_EXTENDEDPRICE']*(1-df['L_DISCOUNT']) - df['PS_SUPPLYCOST']*df['L_QUANTITY']

results = df.groupby(['N_NAME', 'O_YEAR'], as_index=False)['AMOUNT'].sum()
results.columns = ['NATION', 'YEAR', 'SUM_PROFIT']

results = results.sort_values(['NATION', 'YEAR'], ascending=[True, False])
results.to_csv('query_output.csv', index=False)
