from pymongo import MongoClient
import pandas as pd

client = MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]

# Load collections
nation = db["nation"]
supplier = db["supplier"]
lineitem = db["lineitem"]
partsupp = db["partsupp"]
orders = db["orders"]
part = db["part"]

# Create DataFrame
data = pd.DataFrame(columns=["NATION", "O_YEAR", "AMOUNT"])

# Iterate through each document
for n in nation.find():
    for s in supplier.find({"S_NATIONKEY": n["N_NATIONKEY"]}):
        for l in lineitem.find({"L_SUPPKEY": s["S_SUPPKEY"]}):
            for ps in partsupp.find({"PS_SUPPKEY": s["S_SUPPKEY"],
                                     "PS_PARTKEY": l["L_PARTKEY"]}):
                for p in part.find({"P_PARTKEY": l["L_PARTKEY"],
                                    "P_NAME": {"$regex": '*dim*'}}):
                    for o in orders.find({"O_ORDERKEY": l["O_ORDERKEY"]}):
                        data = data.append({
                            "NATION": n["N_NAME"],
                            "O_YEAR": o["O_ORDERDATE"].year,
                            "AMOUNT": l["L_EXTENDEDPRICE"] * 
                            (1 - l["L_DISCOUNT"]) - ps["PS_SUPPLYCOST"] * l["L_QUANTITY"]
                        }, ignore_index=True)

# Group by, sum and sort values
result = data.groupby(['NATION', 'O_YEAR']).sum().sort_values(by=['NATION', 'O_YEAR'], 
ascending=[True, False])

# Write the Data Frame to CSV file
result.to_csv("query_output.csv")
