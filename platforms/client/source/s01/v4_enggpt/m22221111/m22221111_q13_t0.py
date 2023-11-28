from pymongo import MongoClient
import csv

# Establishing connection to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Accessing the collections
customers = db['customer']
orders = db['orders']

# Conduct the analysis
pipeline = [
    {
        '$lookup': {
            'from': 'orders',
            'localField': 'C_CUSTKEY',
            'foreignField': 'O_CUSTKEY',
            'as': 'C_ORDERS'
        }
    },
    {
        '$project': {
            'C_CUSTKEY': 1,
            'C_ORDERS': {
                '$filter': {
                    'input': '$C_ORDERS',
                    'as': 'order',
                    'cond': {
                        '$and': [
                            {'$not': {'$regexMatch': {'input': '$$order.O_COMMENT', 'regex': 'pending|deposits'}}}
                        ]
                    }
                }
            }
        }
    },
    {
        '$project': {
            'C_CUSTKEY': 1,
            'C_COUNT': {'$size': '$C_ORDERS'}
        }
    },
    {
        '$group': {
            '_id': '$C_COUNT',
            'CUSTDIST': {'$sum': 1}
        }
    },
    {
        '$sort': {
            'CUSTDIST': -1,
            '_id': -1
        }
    }
]

# Executing the pipeline
results = list(customers.aggregate(pipeline))

# Writing results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['C_COUNT', 'CUSTDIST']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for result in results:
        writer.writerow({'C_COUNT': result['_id'], 'CUSTDIST': result['CUSTDIST']})

# Closing the connection.
client.close()
