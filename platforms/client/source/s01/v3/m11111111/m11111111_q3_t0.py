import csv
import pymongo
from pymongo import MongoClient
import mysql.connector

# Function for executing MongoDB commands
def mongo_command(client, command, db, collection):
    db = client[db]
    collection = db[collection]
    return collection.aggregate(command)

# Connect Mongo
client = MongoClient("mongodb+srv://<username>:<password>@mongodb:27017/tpch")
db = client['tpch']

# Execute and fetch MongoDB commands
command = [
        {"$match": {"C_MKTSEGMENT": "BUILDING"}},
        {"$lookup": {
            "from": "orders",
            "localField": "C_CUSTKEY",
            "foreignField": "O_CUSTKEY",
            "as": "matching_orders"
        }},
        {"$unwind": "$matching_orders"},
        {"$lookup": {
            "from": "lineitem",
            "localField": "matching_orders.O_ORDERKEY",
            "foreignField": "L_ORDERKEY",
            "as": "matching_lineitems"
        }},
        {"$unwind": "$matching_lineitems"},
        {"$match": {"matching_orders.O_ORDERDATE": {"$lt": '1995-03-15'}, "matching_lineitems.L_SHIPDATE": {"$gt": '1995-03-15'}}},
        {"$group": {"_id": {"orderKey": "$L_ORDERKEY", "orderDate": "$matching_orders.O_ORDERDATE", "shipPriority": "$matching_orders.O_SHIPPRIORITY"}, "REVENUE": {"$sum": {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]}}}},
        {"$sort": {"REVENUE": -1, "_id.orderDate": 1}}
    ]
mongo_result = list(mongo_command(client, command, 'tpch', 'customer'))

# Write result to CSV
keys = mongo_result[0].keys()

with open('query_output.csv', 'w', newline='') as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(mongo_result)
