from pymongo import MongoClient
import csv

# Connect to the MongoDB server
client = MongoClient("mongodb", 27017)
db = client.tpch

# Pipeline for aggregation query
pipeline = [
    {
        '$lookup': {
            'from': 'supplier',
            'localField': 'N_NATIONKEY',
            'foreignField': 'S_NATIONKEY',
            'as': 'suppliers'
        }
    }, 
    {
        '$match': {
            'N_NAME': 'CANADA'
        }
    }, 
    {
        '$unwind': '$suppliers'
    }, 
    {
        '$lookup': {
            'from': 'partsupp',
            'localField': 'suppliers.S_SUPPKEY',
            'foreignField': 'PS_SUPPKEY',
            'as': 'partsupplies'
        }
    }, 
    {
        '$lookup': {
            'from': 'part',
            'localField': 'partsupplies.PS_PARTKEY',
            'foreignField': 'P_PARTKEY',
            'as': 'parts'
        }
    }, 
    {
        '$match': {
            'parts.P_NAME': {
                '$regex': '^forest'
            }
        }
    }, 
    {
        '$lookup': {
            'from': 'lineitem',
            'localField': 'partsupplies.PS_PARTKEY',
            'foreignField': 'L_PARTKEY',
            'as': 'lineitems'
        }
    }, 
    {
        '$match': {
            'lineitems.L_SHIPDATE': {
                '$gte': '1994-01-01',
                '$lt': '1995-01-01'
            }
        }
    }, 
    {
        '$project': {
            'S_NAME': '$suppliers.S_NAME',
            'S_ADDRESS': '$suppliers.S_ADDRESS',
            'half_total_quantity': {
                '$multiply': [
                    {'$divide': [1, 2]},
                    {
                        '$sum': '$lineitems.L_QUANTITY'
                    }
                ]
            },
            'PS_AVAILQTY': '$partsupplies.PS_AVAILQTY'
        }
    }, 
    {
        '$match': {
            '$expr': {
                '$gte': ['$PS_AVAILQTY', '$half_total_quantity']
            }
        }
    }, 
    {
        '$group': {
            '_id': {'S_NAME': '$S_NAME', 'S_ADDRESS': '$S_ADDRESS'}
        }
    }, 
    {
        '$sort': {
            '_id.S_NAME': 1
        }
    }
]

# Execute the aggregation query
result = list(db.nation.aggregate(pipeline))

# Save the results to 'query_output.csv'
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['S_NAME', 'S_ADDRESS']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for doc in result:
        writer.writerow({'S_NAME': doc['_id']['S_NAME'], 'S_ADDRESS': doc['_id']['S_ADDRESS']})
