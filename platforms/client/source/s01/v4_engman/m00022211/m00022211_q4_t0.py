from pymongo import MongoClient
import csv

# Connect to Mongo DB
client = MongoClient('mongodb', 27017)
db = client['tpch']
orders_collection = db['orders']
lineitem_collection = db['lineitem']

# Perform the aggregation query
pipeline = [
    {
        '$lookup': {
            'from': "lineitem",
            'localField': "O_ORDERKEY",
            'foreignField': "L_ORDERKEY",
            'as': "lineitems"
        }
    },
    {
        '$match': {
            'O_ORDERDATE': {
                '$gte': "1993-07-01",
                '$lt': "1993-10-01"
            },
            'lineitems': {
                '$elemMatch': {
                    'L_RECEIPTDATE': {'$gt': "$$ROOT.lineitems.L_COMMITDATE"}
                }
            }
        }
    },
    {
        '$group': {
            '_id': "$O_ORDERPRIORITY",
            'ORDER_COUNT': {'$sum': 1}
        }
    },
    {
        '$sort': {'_id': 1}
    },
    {
        '$project': {
            'O_ORDERPRIORITY': '$_id',
            'ORDER_COUNT': 1,
            '_id': 0
        }
    }
]

results = list(orders_collection.aggregate(pipeline))

# Write the results to CSV file
with open('query_output.csv', mode='w') as file:
    csv_writer = csv.DictWriter(file, fieldnames=['ORDER_COUNT', 'O_ORDERPRIORITY'])
    csv_writer.writeheader()
    for result in results:
        csv_writer.writerow({
            'ORDER_COUNT': result['ORDER_COUNT'],
            'O_ORDERPRIORITY': result['O_ORDERPRIORITY']
        })

client.close()
