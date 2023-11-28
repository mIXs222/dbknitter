from pymongo import MongoClient
import csv
from datetime import datetime

# Establish a connection to the MongoDB server
client = MongoClient('mongodb', 27017)

# Select the database
db = client['tpch']

# Access the collections
orders = db['orders']
lineitem = db['lineitem']

# Convert the date strings to datetime objects for the query
start_date = datetime.strptime('1993-07-01', '%Y-%m-%d')
end_date = datetime.strptime('1993-10-01', '%Y-%m-%d')

# Run the query
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
            'O_ORDERDATE': {'$gte': start_date, '$lte': end_date},
            'lineitems': {'$elemMatch': {'L_COMMITDATE': {'$lt': '$L_RECEIPTDATE'}}}
        }
    },
    {
        '$group': {
            '_id': '$O_ORDERPRIORITY',
            'order_count': {'$sum': 1}
        }
    },
    {
        '$sort': {'_id': 1}
    }
]

results = list(orders.aggregate(pipeline))

# Write results to CSV
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['O_ORDERPRIORITY', 'order_count'])
    for result in results:
        writer.writerow([result['_id'], result['order_count']])

# Close the connection to the MongoDB server
client.close()
