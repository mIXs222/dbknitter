from pymongo import MongoClient
import csv
from datetime import datetime, timedelta

# Connect to MongoDB instance
client = MongoClient("mongodb", 27017)
db = client["tpch"]

# Prepare the pipeline for the aggregation framework
seven_years_ago = datetime.now() - timedelta(days=7*365)

pipeline = [
    {'$match': {
        'C_PHONE': {'$regex': '^(20|40|22|30|39|42|21)'},
        'C_ACCTBAL': {'$gt': 0}
    }},
    {'$group': {
        '_id': {'CNTRYCODE': {'$substr': ['$C_PHONE', 0, 2]}},
        'average_balance': {'$avg': '$C_ACCTBAL'},
        'all_customers': {'$push': '$$ROOT'}
    }},
    {'$project': {
        'all_customers': {
            '$filter': {
                'input': '$all_customers',
                'as': 'customer',
                'cond': {'$and': [
                    {'$not': [{'$eq': ['$$customer.O_ORDERDATE', None]}]},
                    {'$gt': ['$$customer.O_ORDERDATE', seven_years_ago]},
                    {'$gt': ['$$customer.C_ACCTBAL', '$average_balance']}
                ]}
            }
        },
        'average_balance': 1
    }},
    {'$unwind': '$all_customers'},
    {'$group': {
        '_id': '$_id.CNTRYCODE',
        'num_of_customers': {'$sum': 1},
        'total_balance': {'$sum': '$all_customers.C_ACCTBAL'},
    }},
    {'$sort': {'_id': 1}}
]

# Run the aggregation pipeline
results = db.customer.aggregate(pipeline)

# Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['CNTRYCODE', 'num_of_customers', 'total_balance']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    writer.writeheader()
    for result in results:
        writer.writerow({
            'CNTRYCODE': result['_id'],
            'num_of_customers': result['num_of_customers'],
            'total_balance': result['total_balance']
        })

# Close MongoDB client
client.close()
