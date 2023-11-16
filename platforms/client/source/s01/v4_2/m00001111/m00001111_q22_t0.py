import pymongo
import pandas as pd
from statistics import mean 

# Set up connection
client = pymongo.MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]

# Table data
customer = pd.DataFrame(list(db["customer"].find()))
orders = pd.DataFrame(list(db["orders"].find()))

# Process data
customer['CNTRYCODE'] = customer['C_PHONE'].str[:2]
desired_codes = ['20', '40', '22', '30', '39', '42', '21']
customer = customer[customer['CNTRYCODE'].isin(desired_codes)]
avg_acctbal = mean([x for x in customer['C_ACCTBAL'] if x > 0])
customer = customer[customer['C_ACCTBAL'] > avg_acctbal]
customer = customer[~customer['C_CUSTKEY'].isin(orders['O_CUSTKEY'])]

# Select and group data
grouped = customer.groupby('CNTRYCODE').agg({'C_CUSTKEY': 'count', 'C_ACCTBAL': 'sum'}).reset_index()
grouped.columns = ['CNTRYCODE', 'NUMCUST', 'TOTACCTBAL']

# Sort and write to CSV
grouped.sort_values(by='CNTRYCODE', inplace=True)
grouped.to_csv('query_output.csv', index=False)
