import pymongo
import csv

# Connect to the MongoDB server
client = pymongo.MongoClient('mongodb://mongodb:27017/')
db = client['tpch']

# Query to fetch the relevant orders
orders_pipeline = [
    {
        '$match': {
            'O_ORDERDATE': {'$gte': '1993-07-01', '$lt': '1993-10-01'}
        }
    },
    {
        '$lookup': {
            'from': 'lineitem',
            'localField': 'O_ORDERKEY',
            'foreignField': 'L_ORDERKEY',
            'as': 'lineitems'
        }
    },
    {
        '$unwind': '$lineitems'
    },
    {
        '$match': {
            'lineitems.L_COMMITDATE': {'$lt': 'lineitems.L_RECEIPTDATE'}
        }
    },
    {
        '$group': {
            '_id': {
                'O_ORDERPRIORITY': '$O_ORDERPRIORITY'
            },
            'late_order_count': {'$sum': 1}
        }
    },
    {
        '$sort': {'_id.O_ORDERPRIORITY': 1}
    }
]

# Run the aggregation pipeline
results = list(db.orders.aggregate(orders_pipeline))

# Write results to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['O_ORDERPRIORITY', 'late_order_count'])
    for result in results:
        writer.writerow([result['_id']['O_ORDERPRIORITY'], result['late_order_count']])
