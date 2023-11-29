from pymongo import MongoClient
import csv

# MongoDB connection setup
client = MongoClient('mongodb', 27017)
db = client['tpch']
orders_collection = db['orders']
lineitem_collection = db['lineitem']

# Query
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
        '$match': {
            'O_ORDERDATE': {'$gte': '1993-07-01', '$lt': '1993-10-01'},
            'lineitems': {'$elemMatch': {'L_COMMITDATE': {'$lt': '$lineitems.L_RECEIPTDATE'}}}
        }
    },
    {
        '$group': {
            '_id': '$O_ORDERPRIORITY',
            'ORDER_COUNT': {'$sum': 1}
        }
    },
    {'$sort': {'_id': 1}}
]

# Execute query and write results to CSV file
result = list(orders_collection.aggregate(pipeline))

# Write to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fields = ['O_ORDERPRIORITY', 'ORDER_COUNT']
    writer = csv.DictWriter(csvfile, fieldnames=fields)

    writer.writeheader()
    for data in result:
        writer.writerow({'O_ORDERPRIORITY': data['_id'], 'ORDER_COUNT': data['ORDER_COUNT']})
