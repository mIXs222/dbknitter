from pymongo import MongoClient
import mysql.connector
import pandas as pd
from pandas.io.json import json_normalize
import os

# Establish connection to mongodb
mongo_client = MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# get data from mongodb tables
supplier = pd.DataFrame(list(mongodb.supplier.find()))
lineitem = pd.DataFrame(list(mongodb.lineitem.find()))

# convert shipdate to datetime
lineitem['L_SHIPDATE'] = pd.to_datetime(lineitem['L_SHIPDATE'])

# Filter and Aggregate lineitem data
revenue0 = lineitem[(lineitem['L_SHIPDATE'] >= '1996-01-01') & (lineitem['L_SHIPDATE'] < '1996-04-01')].groupby('L_SUPPKEY').agg({'L_EXTENDEDPRICE': 'sum'}).rename(columns={'L_EXTENDEDPRICE': 'TOTAL_REVENUE'})
revenue0['TOTAL_REVENUE'] = revenue0['TOTAL_REVENUE'] * (1 - lineitem['L_DISCOUNT'])

# Join Supplier and revenue0 data
result = pd.merge(supplier, revenue0, left_on='S_SUPPKEY', right_on='SUPPLIER_NO', how='inner')

# Filter data where TOTAL_REVENUE is maximum
result = result[result['TOTAL_REVENUE'] == result['TOTAL_REVENUE'].max()]

# Write output to file
result.to_csv('query_output.csv', index=False)

