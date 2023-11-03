from pymongo import MongoClient
from datetime import datetime
import csv

client = MongoClient("mongodb://mongodb:27017/")
db = client['tpch']

customer = db.customer.find()
orders = db.orders.find()
lineitem = db.lineitem.find()
supplier = db.supplier.find()
nation = db.nation.find()
region = db.region.find()

data = []

for c in customer:
    for o in orders:
        if c["C_CUSTKEY"] == o["O_CUSTKEY"]:
            for l in lineitem:
                if l["L_ORDERKEY"] == o["O_ORDERKEY"]:
                    for s in supplier:
                        if l["L_SUPPKEY"] == s["S_SUPPKEY"] and c["C_NATIONKEY"] == s["S_NATIONKEY"]:
                            for n in nation:
                                if s["S_NATIONKEY"] == n["N_NATIONKEY"]:
                                    for r in region:
                                        if n["N_REGIONKEY"] == r["R_REGIONKEY"] and r["R_NAME"] == 'ASIA' and datetime.strptime(o["O_ORDERDATE"], "%Y-%m-%d") >= datetime.strptime('1990-01-01', "%Y-%m-%d") and datetime.strptime(o["O_ORDERDATE"], "%Y-%m-%d") < datetime.strptime('1995-01-01', "%Y-%m-%d"):
                                            revenue = l["L_EXTENDEDPRICE"] * (1 - l["L_DISCOUNT"])
                                            data.append([n["N_NAME"], revenue])

data.sort(key = lambda x: x[1], reverse=True)

with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(data)
