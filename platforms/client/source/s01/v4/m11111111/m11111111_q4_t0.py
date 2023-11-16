from pymongo import MongoClient
import csv

# Establish connection to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Prepare the query for MongoDB
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
            'as': 'matching_lineitems'
        }
    },
    {
        '$match': {
            'matching_lineitems': {
                '$elemMatch': {
                    'L_COMMITDATE': {'$lt': '$$L_RECEIPTDATE'}
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
        '$sort': {
            '_id': 1
        }
    },
]

# Execute the query
orders_cursor = db.orders.aggregate(pipeline)

# Write result to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['O_ORDERPRIORITY', 'ORDER_COUNT']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for doc in orders_cursor:
        writer.writerow({
            'O_ORDERPRIORITY': doc['_id'],
            'ORDER_COUNT': doc['ORDER_COUNT']
        })

# Close the connection to MongoDB
client.close()
