from pymongo import MongoClient
import csv

# Connect to the MongoDB server
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Define the pipeline for the aggregation query
pipeline = [
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
        '$match': {
            'lineitems': {
                '$elemMatch': {
                    'L_COMMITDATE': {'$lt': '$$ROOT.L_RECEIPTDATE'}
                }
            }
        }
    },
    {
        '$group': {
            '_id': '$O_ORDERPRIORITY',
            'ORDER_COUNT': {'$sum': 1}
        }
    },
    {
        '$sort': {'_id': 1}
    }
]

# Execute the aggregation query
result = db.orders.aggregate(pipeline)

# Write the result to query_output.csv
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['O_ORDERPRIORITY', 'ORDER_COUNT'])
    for doc in result:
        writer.writerow([doc['_id'], doc['ORDER_COUNT']])
