from pymongo import MongoClient
import csv

# MongoDB connection parameters
db_name = 'tpch'
port = 27017
hostname = 'mongodb'

# Connect to the MongoDB server
client = MongoClient(hostname, port)
db = client[db_name]

# We need to aggregate data from `customer` and `orders`
# We'll join them by customer key and count the orders, excluding certain order comments
pipeline = [
    {
        '$lookup': {
            'from': 'orders',
            'localField': 'C_CUSTKEY',
            'foreignField': 'O_CUSTKEY',
            'as': 'customer_orders'
        }
    },
    {
        '$project': {
            'customer_orders': {
                '$filter': {
                    'input': '$customer_orders',
                    'as': 'order',
                    'cond': {
                        '$and': [
                            {'$ne': ['$$order.O_ORDERSTATUS', 'pending']},
                            {'$ne': ['$$order.O_ORDERSTATUS', 'deposits']},
                            {'$not': {'$regexMatch': {'input': '$$order.O_COMMENT', 'regex': 'pending'}}},
                            {'$not': {'$regexMatch': {'input': '$$order.O_COMMENT', 'regex': 'deposits'}}}
                        ]
                    }
                }
            }
        }
    },
    {
        '$group': {
            '_id': {'num_orders': {'$size': '$customer_orders'}},
            'customer_count': {'$sum': 1}
        }
    },
    {
        '$sort': {'_id': 1}
    }
]

results = list(db.customer.aggregate(pipeline))

# Write results to CSV file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['num_orders', 'customer_count'])

    for result in results:
        writer.writerow([result['_id']['num_orders'], result['customer_count']])

# Close the MongoDB connection
client.close()
