# query.py
from pymongo import MongoClient
import csv
from datetime import datetime

# Define the connection parameters
HOSTNAME = 'mongodb'
PORT = 27017
DATABASE = 'tpch'

# Connect to the MongoDB server
client = MongoClient(HOSTNAME, PORT)
db = client[DATABASE]

# Define the date range
start_date = datetime(1993, 7, 1)
end_date = datetime(1993, 10, 1)

# Define the query
query = [
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
            'O_ORDERDATE': {'$gte': start_date, '$lt': end_date},
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
            'OrderCount': {'$sum': 1}
        }
    },
    {
        '$sort': {'_id': 1}
    }
]

# Execute the query
result = db.orders.aggregate(query)

# Write to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['O_ORDERPRIORITY', 'OrderCount']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for entry in result:
        writer.writerow({'O_ORDERPRIORITY': entry['_id'], 'OrderCount': entry['OrderCount']})

# Close the MongoDB client
client.close()
