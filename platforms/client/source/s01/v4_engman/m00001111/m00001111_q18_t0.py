from pymongo import MongoClient
import csv

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client.tpch

# Perform aggregation
pipeline = [
    {
        '$lookup': {
            'from': 'lineitem',
            'localField': 'O_ORDERKEY',
            'foreignField': 'L_ORDERKEY',
            'as': 'lineitems'
        }
    },
    {
        '$project': {
            'O_CUSTKEY': 1,
            'O_ORDERKEY': 1,
            'O_ORDERDATE': 1,
            'O_TOTALPRICE': 1,
            'total_quantity': { '$sum': '$lineitems.L_QUANTITY' }
        }
    },
    {
        '$match': {
            'total_quantity': { '$gt': 300 }
        }
    },
    {
        '$lookup': {
            'from': 'customer',
            'localField': 'O_CUSTKEY',
            'foreignField': 'C_CUSTKEY',
            'as': 'customer_info'
        }
    },
    {
        '$unwind': '$customer_info'
    },
    {
        '$project': {
            'customer_name': '$customer_info.C_NAME',
            'customer_key': '$O_CUSTKEY',
            'order_key': '$O_ORDERKEY',
            'order_date': '$O_ORDERDATE',
            'total_price': '$O_TOTALPRICE',
            'quantity': '$total_quantity'
        }
    },
    { '$sort': { 'total_price': -1, 'order_date': 1 } },
]

result = list(db.orders.aggregate(pipeline))

# Write to CSV
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['customer_name', 'customer_key', 'order_key', 'order_date', 'total_price', 'quantity'])

    for record in result:
        writer.writerow([
            record['customer_name'],
            record['customer_key'],
            record['order_key'],
            record['order_date'],
            record['total_price'],
            record['quantity']
        ])
