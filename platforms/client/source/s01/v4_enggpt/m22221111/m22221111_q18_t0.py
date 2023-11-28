from pymongo import MongoClient
import csv

# MongoDB connection parameters
host = 'mongodb'
port = 27017
db_name = 'tpch'

# Connect to the MongoDB server
client = MongoClient(host, port)
db = client[db_name]
    
# Subquery to find orders with total quantity over 300
subquery = db.lineitem.aggregate([
    {"$group": {
        "_id": "$L_ORDERKEY",
        "total_qty": {"$sum": "$L_QUANTITY"}
    }},
    {"$match": {
        "total_qty": {"$gt": 300}
    }}
])

# Extract the qualifying order keys from the subquery results
qualifying_order_keys = set(doc['_id'] for doc in subquery)

# Perform the main query
main_query = db.orders.aggregate([
    {"$match": {
        "O_ORDERKEY": {"$in": list(qualifying_order_keys)}
    }},
    {"$lookup": {
        "from": "customer",
        "localField": "O_CUSTKEY",
        "foreignField": "C_CUSTKEY",
        "as": "customer"
    }},
    {"$lookup": {
        "from": "lineitem",
        "localField": "O_ORDERKEY",
        "foreignField": "L_ORDERKEY",
        "as": "lineitems"
    }},
    {"$unwind": "$customer"},
    {"$unwind": "$lineitems"},
    {"$group": {
        "_id": {
            "C_NAME": "$customer.C_NAME",
            "C_CUSTKEY": "$O_CUSTKEY",
            "O_ORDERKEY": "$O_ORDERKEY",
            "O_ORDERDATE": "$O_ORDERDATE",
            "O_TOTALPRICE": "$O_TOTALPRICE"
        },
        "sum_qty": {"$sum": "$lineitems.L_QUANTITY"}
    }},
    {"$sort": {
        "_id.O_TOTALPRICE": -1,
        "_id.O_ORDERDATE": 1
    }}
])

# Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'SUM_QTY']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for doc in main_query:
        writer.writerow({
            'C_NAME': doc['_id']['C_NAME'],
            'C_CUSTKEY': doc['_id']['C_CUSTKEY'],
            'O_ORDERKEY': doc['_id']['O_ORDERKEY'],
            'O_ORDERDATE': doc['_id']['O_ORDERDATE'],
            'O_TOTALPRICE': doc['_id']['O_TOTALPRICE'],
            'SUM_QTY': doc['sum_qty']
        })

# Close the connection to the MongoDB server
client.close()
