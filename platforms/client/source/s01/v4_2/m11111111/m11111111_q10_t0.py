import pymongo
from pymongo import MongoClient
import pandas as pd
from pandas import DataFrame
from dateutil.parser import parse

# Establish mongodb connection
client = MongoClient("mongodb", 27017)
db = client['tpch']

# Query data from mongodb databases
nation = DataFrame(list(db.nation.find({},{ "_id": 0})))
customer = DataFrame(list(db.customer.find({},{ "_id": 0})))
orders = DataFrame(list(db.orders.find({},{ "_id": 0})))
lineitem = DataFrame(list(db.lineitem.find({},{ "_id": 0})))

# Convert date strings to datetime objects
orders['O_ORDERDATE'] = pd.to_datetime(orders['O_ORDERDATE'])

# Merge dataframes
result = pd.merge(customer, orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
result = pd.merge(result, lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
result = pd.merge(result, nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Apply WHERE clauses
result = result[
    (result['O_ORDERDATE'] >= '1993-10-01') &
    (result['O_ORDERDATE'] < '1994-01-01') &
    (result['L_RETURNFLAG'] == 'R')
]

# Compute REVENUE
result['REVENUE'] = result['L_EXTENDEDPRICE'] * (1 - result['L_DISCOUNT'])

# Select necessary columns
result = result[[
    'C_CUSTKEY',
    'C_NAME',
    'REVENUE',
    'C_ACCTBAL',
    'N_NAME',
    'C_ADDRESS',
    'C_PHONE',
    'C_COMMENT'
]]

# Group by necessary columns and compute REVENUE
result = result.groupby([
    'C_CUSTKEY',
    'C_NAME',
    'C_ACCTBAL',
    'N_NAME',
    'C_ADDRESS',
    'C_PHONE',
    'C_COMMENT'
]).agg({'REVENUE': 'sum'}).reset_index()

# Sort by necessary columns
result = result.sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[True, True, True, False])

# Write to CSV
result.to_csv('query_output.csv', index=False)
