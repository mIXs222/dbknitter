# python_code.py
import pymongo
import csv

# Connection details
host = "mongodb"
port = 27017
database_name = "tpch"

# Connect to MongoDB
client = pymongo.MongoClient(host=host, port=port)
db = client[database_name]

# Perform a left outer join to find all customers and their number of orders
pipeline = [
    {
        '$lookup': {
            'from': 'orders',
            'localField': 'C_CUSTKEY',
            'foreignField': 'O_CUSTKEY',
            'as': 'orders'
        }
    },
    {
        '$project': {
            'C_CUSTKEY': 1,
            'num_of_orders': {
                '$size': {
                    '$filter': {
                        'input': '$orders',
                        'as': 'order',
                        'cond': {
                            '$and': [
                                {'$ne': ['$$order.O_ORDERSTATUS', 'pending']},
                                {'$ne': ['$$order.O_ORDERSTATUS', 'deposits']},
                                {'$not': [{'$regexMatch': {'input': '$$order.O_COMMENT', 'regex': 'pending|deposits'}}]}
                            ]
                        }
                    }
                }
            }
        }
    },
    {
        '$group': {
            '_id': '$num_of_orders',
            'customer_count': {'$sum': 1}
        }
    },
    {
        '$sort': {'_id': 1}
    }
]

# Execute the aggregation pipeline
results = list(db.customer.aggregate(pipeline))

# Write results to CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Number of Orders', 'Customer Count'])

    for result in results:
        writer.writerow([result['_id'], result['customer_count']])
