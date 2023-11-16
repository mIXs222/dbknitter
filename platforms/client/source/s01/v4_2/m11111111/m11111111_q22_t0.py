# query.py

from pymongo import MongoClient
import pandas as pd
from pandas import DataFrame

client = MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]

customer = list(db.customer.find({}, {"C_CUSTKEY":1,"C_PHONE": 1, "C_ACCTBAL": 1, "_id":0}))
orders = list(db.orders.find({}, {"O_CUSTKEY": 1, "_id":0}))

customer = DataFrame(customer)
orders = DataFrame(orders)
unique_orders_custkey = orders['O_CUSTKEY'].drop_duplicates()

df = pd.DataFrame(columns=['C_CUSTKEY', 'C_PHONE', 'C_ACCTBAL'])

cust_no_orders = customer[~customer.C_CUSTKEY.isin(unique_orders_custkey)]

for index, row in cust_no_orders.iterrows():
    if row['C_ACCTBAL'] > 0 and row['C_PHONE'][0:2] in ['20', '40', '22', '30', '39', '42', '21']:
        df = df.append(row)

avg_acct_bal = df[df['C_ACCTBAL'] > 0]['C_ACCTBAL'].mean()
df_above_avg = df[df['C_ACCTBAL'] > avg_acct_bal]

result = df_above_avg.groupby(df_above_avg['C_PHONE'].str[0:2]).agg({'C_CUSTKEY':'count', 'C_ACCTBAL':'sum'})

result.to_csv('query_output.csv')
