# local_supplier_volume_query.py
import pymongo
import csv
from datetime import datetime

# MongoDB connection
client = pymongo.MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]

# Filter for dates between 1990-01-01 and 1995-01-01 and region ASIA
date_start = datetime(1990, 1, 1)
date_end = datetime(1995, 1, 1)
asia_region = db.region.find_one({"R_NAME": "ASIA"})
if not asia_region:
    raise ValueError("The ASIA region was not found in the database.")

# Find all nations in ASIA region
asia_nations = list(db.nation.find({"N_REGIONKEY": asia_region["R_REGIONKEY"]}))

# Find all suppliers from nations within the ASIA region
asia_nation_keys = [nation["N_NATIONKEY"] for nation in asia_nations]
asia_suppliers = list(db.supplier.find({"S_NATIONKEY": {"$in": asia_nation_keys}}))

# Find customers from the nations within ASIA region
asia_customers = list(db.customer.find({"C_NATIONKEY": {"$in": asia_nation_keys}}))

# Find orders made by those customers between specified dates
asia_customer_keys = [customer["C_CUSTKEY"] for customer in asia_customers]
orders = db.orders.find(
    {
        "O_CUSTKEY": {"$in": asia_customer_keys},
        "O_ORDERDATE": {"$gte": date_start, "$lte": date_end}
    }
)

# Create a list of qualifying order keys
order_keys = [order["O_ORDERKEY"] for order in orders]

# Find lineitems associated with these orders
lineitems = db.lineitem.find({"L_ORDERKEY": {"$in": order_keys}})

# Compute the revenue volume for each lineitem
lineitem_revenues = {}
for item in lineitems:
    if item["L_SUPPKEY"] in [supplier["S_SUPPKEY"] for supplier in asia_suppliers]:
        revenue = item["L_EXTENDEDPRICE"] * (1 - item["L_DISCOUNT"])
        nation_key = next(supplier["S_NATIONKEY"] for supplier in asia_suppliers if supplier["S_SUPPKEY"] == item["L_SUPPKEY"])
        if nation_key in lineitem_revenues:
            lineitem_revenues[nation_key] += revenue
        else:
            lineitem_revenues[nation_key] = revenue

# Sort nations by revenue
sorted_nation_revenues = sorted(lineitem_revenues.items(), key=lambda x: x[1], reverse=True)

# Write results to a file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['N_NATIONKEY', 'REVENUE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for nation_key, revenue in sorted_nation_revenues:
        writer.writerow({'N_NATIONKEY': nation_key, 'REVENUE': revenue})
