import pymongo
import pandas as pd
from pymongo import MongoClient
from pandas import DataFrame
from datetime import datetime

# connect to MongoDB
client = MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]

# list to store query output
output_data = []

customers = db["customer"].find()
orders = db["orders"].find()
lineitems = db["lineitem"].find({"L_RETURNFLAG": "R"})
nations = db["nation"].find()

for customer in customers:
    for order in orders:
        if order["O_CUSTKEY"] != customer["C_CUSTKEY"]:
            continue
        for lineitem in lineitems:
            if lineitem["L_ORDERKEY"] != order["O_ORDERKEY"]:
                continue
            if not (datetime.strptime(order["O_ORDERDATE"], '%Y-%m-%d') >= datetime.strptime('1993-10-01', '%Y-%m-%d') and datetime.strptime(order["O_ORDERDATE"], '%Y-%m-%d') < datetime.strptime('1994-01-01', '%Y-%m-%d')):
                continue
            for nation in nations:
                if nation["N_NATIONKEY"] != customer["C_NATIONKEY"]:
                    continue
                revenue = lineitem["L_EXTENDEDPRICE"] * (1 - lineitem["L_DISCOUNT"])
                output_data.append((customer["C_CUSTKEY"], customer["C_NAME"], revenue, customer["C_ACCTBAL"], nation["N_NAME"], customer["C_ADDRESS"], customer["C_PHONE"], customer["C_COMMENT"]))

# converting the list to pandas DataFrame
df = DataFrame(output_data, columns=["C_CUSTKEY", "C_NAME", "REVENUE", "C_ACCTBAL", "N_NAME", "C_ADDRESS", "C_PHONE", "C_COMMENT"])

# group by and sort
df = df.groupby(["C_CUSTKEY", "C_NAME", "C_ACCTBAL", "C_PHONE", "N_NAME", "C_ADDRESS", "C_COMMENT"]).sum().reset_index()
df = df.sort_values(by=["REVENUE", "C_CUSTKEY", "C_NAME", "C_ACCTBAL"], ascending=[1,1,1,0])

# save to csv
df.to_csv('query_output.csv', index=False)
