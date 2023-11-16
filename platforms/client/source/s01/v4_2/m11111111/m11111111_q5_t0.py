# query.py
import pymongo
import csv
from datetime import datetime
from operator import itemgetter

# connection to mongodb
client = pymongo.MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]

# initialize individual collections
customer_col = db["customer"]
orders_col = db["orders"]
lineitem_col = db["lineitem"]
supplier_col = db["supplier"]
nation_col = db["nation"]
region_col = db["region"]

results = []
for customer in customer_col.find({}):
    for order in orders_col.find({"O_CUSTKEY": customer["C_CUSTKEY"]}):
        if datetime.strptime(order["O_ORDERDATE"], "%Y-%m-%d") >= datetime(1990, 1, 1) and datetime.strptime(order["O_ORDERDATE"], "%Y-%m-%d") < datetime(1995, 1, 1):
            for lineitem in lineitem_col.find({"L_ORDERKEY": order["O_ORDERKEY"]}):
                for supplier in supplier_col.find({"S_SUPPKEY": lineitem["L_SUPPKEY"]}):
                    if customer["C_NATIONKEY"] == supplier["S_NATIONKEY"]:
                        for nation in nation_col.find({"N_NATIONKEY": supplier["S_NATIONKEY"]}):
                            for region in region_col.find({"R_REGIONKEY": nation["N_REGIONKEY"]}):
                                if region["R_NAME"] == "ASIA":
                                    revenue = lineitem["L_EXTENDEDPRICE"] * (1 - lineitem["L_DISCOUNT"])
                                    results.append([nation["N_NAME"], revenue])

# group by N_NAME
grouped_result = {}
for item in results:
    if item[0] in grouped_result:
        grouped_result[item[0]] += item[1]
    else:
        grouped_result[item[0]] = item[1]

# sort by REVENUE
sorted_result = sorted(grouped_result.items(), key=itemgetter(1), reverse=True)

# write results to csv
with open("query_output.csv", "w") as file:
    writer = csv.writer(file)
    writer.writerow(["N_NAME", "REVENUE"])
    for row in sorted_result:
        writer.writerow(row)
