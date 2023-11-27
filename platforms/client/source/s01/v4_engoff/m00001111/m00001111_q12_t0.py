from pymongo import MongoClient
from datetime import datetime
import csv

# MongoDB connection parameters
DATABASE_NAME = 'tpch'
MONGODB_PORT = 27017
MONGODB_HOST = 'mongodb'

# MongoDB connection
client = MongoClient(MONGODB_HOST, MONGODB_PORT)
db = client[DATABASE_NAME]

# Dates for the query
start_date = datetime(1994, 1, 1)
end_date = datetime(1995, 1, 1)

# Aggregation pipeline
pipeline = [
    {
        '$lookup': {
            'from': 'orders',
            'localField': 'L_ORDERKEY',
            'foreignField': 'O_ORDERKEY',
            'as': 'order_info'
        }
    },
    {'$unwind': '$order_info'},
    {'$match': {
        'L_SHIPMODE': {'$in': ['MAIL', 'SHIP']},
        'L_RECEIPTDATE': {'$gte': start_date, '$lt': end_date},
        'L_COMMITDATE': {'$lt': 'L_RECEIPTDATE'},
        'L_SHIPDATE': {'$lt': 'L_COMMITDATE'},
        'order_info.O_ORDERPRIORITY': {'$in': ['URGENT', 'HIGH']}
    }},
    {
        '$group': {
            '_id': {
                'ShipMode': '$L_SHIPMODE',
                'Priority': {
                    '$cond': [{'$in': ['$order_info.O_ORDERPRIORITY', ['URGENT', 'HIGH']]}, 'HIGH/URGENT', 'OTHER']
                }
            },
            'LateLineItemCount': {'$sum': 1}
        }
    },
    {
        '$sort': {
            '_id.ShipMode': 1,
            '_id.Priority': 1
        }
    }
]

# Execute the aggregation
results = list(db.lineitem.aggregate(pipeline))

# Write output to a CSV file
output_file = 'query_output.csv'
with open(output_file, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['ShipMode', 'Priority', 'LateLineItemCount'])

    for result in results:
        writer.writerow([result['_id']['ShipMode'],
                         result['_id']['Priority'],
                         result['LateLineItemCount']])

client.close()
