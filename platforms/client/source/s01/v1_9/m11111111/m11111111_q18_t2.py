from pymongo import MongoClient
import csv

# Connect to MongoDB
client = MongoClient("mongodb://mongodb:27017/")

# Define database
db = client["tpch"]

# Define collections
customers = db["customer"]
orders = db["orders"]
lineitem = db["lineitem"]

# Query to get the order keys with total quantity > 300
pipeline1 = [
    {"$group": {"_id": "$L_ORDERKEY", "total_quantity": {"$sum": "$L_QUANTITY"}}},
    {"$match": {"total_quantity": {"$gt": 300}}},
]

orderkeys = list(lineitem.aggregate(pipeline1))

orderkeys_list = [ok["_id"] for ok in orderkeys]

# Query to get the main data
pipeline2 = [
    {
        "$lookup": {
            "from": "orders",
            "localField": "C_CUSTKEY",
            "foreignField": "O_CUSTKEY",
            "as": "customer_order",
        }
    },
    {"$unwind": "$customer_order"},
    {"$match": {"customer_order.O_ORDERKEY": {"$in": orderkeys_list}}},
    {
        "$lookup": {
            "from": "lineitem",
            "localField": "customer_order.O_ORDERKEY",
            "foreignField": "L_ORDERKEY",
            "as": "order_lineitem",
        }
    },
    {"$unwind": "$order_lineitem"},
    {
        "$group": {
            "_id": {
                "C_NAME": "$C_NAME",
                "C_CUSTKEY": "$C_CUSTKEY",
                "O_ORDERKEY": "$customer_order.O_ORDERKEY",
                "O_ORDERDATE": "$customer_order.O_ORDERDATE",
                "O_TOTALPRICE": "$customer_order.O_TOTALPRICE",
            },
            "total_quantity": {"$sum": "$order_lineitem.L_QUANTITY"},
        }
    },
    {"$sort": {"_id.O_TOTALPRICE": -1, "_id.O_ORDERDATE": 1}},
]

results = list(customers.aggregate(pipeline2))

# Write the output to CSV
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["C_NAME", "C_CUSTKEY", "O_ORDERKEY", "O_ORDERDATE", "O_TOTALPRICE", "TOTAL_QUANTITY"])
    for result in results:
        writer.writerow(
            list(result["_id"].values()) + [result["total_quantity"]]
        )
