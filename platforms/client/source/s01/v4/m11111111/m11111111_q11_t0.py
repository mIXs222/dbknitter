# query_mongodb.py
from pymongo import MongoClient
import csv

# Establish a connection to the MongoDB server
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Aggregation Pipeline to emulate the SQL query
pipeline = [
    {
        '$lookup': {
            'from': 'supplier',
            'localField': 'PS_SUPPKEY',
            'foreignField': 'S_SUPPKEY',
            'as': 'supplier'
        }
    },
    {'$unwind': '$supplier'},
    {
        '$lookup': {
            'from': 'nation',
            'localField': 'supplier.S_NATIONKEY',
            'foreignField': 'N_NATIONKEY',
            'as': 'nation'
        }
    },
    {'$unwind': '$nation'},
    {'$match': {'nation.N_NAME': 'GERMANY'}},
    {
        '$group': {
            '_id': '$PS_PARTKEY',
            'VALUE': {'$sum': {'$multiply': ['$PS_SUPPLYCOST', '$PS_AVAILQTY']}}
        }
    },
    {
        '$project': {
            'PS_PARTKEY': '$_id',
            'VALUE': 1,
            '_id': 0
        }
    }
]

# Calculate the threshold value as per the subquery
threshold_pipeline = [
    {'$match': {'nation.N_NAME': 'GERMANY'}},
    {
        '$group': {
            '_id': None,
            'total_value': {'$sum': {'$multiply': ['$PS_SUPPLYCOST', '$PS_AVAILQTY']}}
        }
    },
    {
        '$project': {
            'threshold': {'$multiply': ['$total_value', 0.0001000000]},
            '_id': 0
        }
    }
]

# Get the threshold value
threshold_result = list(db['partsupp'].aggregate(threshold_pipeline + pipeline))
threshold = threshold_result[0]['threshold'] if threshold_result else 0

# Apply having clause by filtering on the threshold
pipeline.append({'$match': {'VALUE': {'$gt': threshold}}})

# Apply ordering
pipeline.append({'$sort': {'VALUE': -1}})

# Execute the query
results = list(db['partsupp'].aggregate(pipeline))

# Write the output to CSV file
with open('query_output.csv', mode='w') as output_file:
    csv_writer = csv.DictWriter(output_file, fieldnames=["PS_PARTKEY", "VALUE"])
    csv_writer.writeheader()
    for data in results:
        csv_writer.writerow(data)

# Close the connection
client.close()
