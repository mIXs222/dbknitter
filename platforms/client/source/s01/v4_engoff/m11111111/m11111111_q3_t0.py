import pymongo
import csv

# MongoDB connection string
client = pymongo.MongoClient("mongodb://mongodb:27017/")

# Connect to the tpch database
db = client["tpch"]

# Perform the query
pipeline = [
    {
        '$lookup': {
            'from': 'orders',
            'localField': 'C_CUSTKEY',
            'foreignField': 'O_CUSTKEY',
            'as': 'customer_orders'
        }
    },
    {'$unwind': '$customer_orders'},
    {
        '$lookup': {
            'from': 'lineitem',
            'localField': 'customer_orders.O_ORDERKEY',
            'foreignField': 'L_ORDERKEY',
            'as': 'lineitems'
        }
    },
    {'$unwind': '$lineitems'},
    {
        '$project': {
            'O_ORDERKEY': '$customer_orders.O_ORDERKEY',
            'O_SHIPPRIORITY': '$customer_orders.O_SHIPPRIORITY',
            'revenue': {
                '$multiply': [
                    '$lineitems.L_EXTENDEDPRICE',
                    {'$subtract': [1, '$lineitems.L_DISCOUNT']}
                ]
            },
            'L_SHIPDATE': '$lineitems.L_SHIPDATE'
        }
    },
    {'$match': {
        'lineitems.L_SHIPDATE': {'$gt': '1995-03-15'},
        'customer_orders.O_ORDERPRIORITY': 'BUILDING'
    }},
    {
        '$group': {
            '_id': '$O_ORDERKEY',
            'O_SHIPPRIORITY': {'$first': '$O_SHIPPRIORITY'},
            'revenue': {'$sum': '$revenue'}
        }
    },
    {'$sort': {'revenue': -1}},
    {'$limit': 1}
]

results = db.customer.aggregate(pipeline)

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['O_ORDERKEY', 'O_SHIPPRIORITY', 'revenue']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for result in results:
        writer.writerow({
            'O_ORDERKEY': result['_id'],
            'O_SHIPPRIORITY': result['O_SHIPPRIORITY'],
            'revenue': round(result['revenue'], 2)
        })
