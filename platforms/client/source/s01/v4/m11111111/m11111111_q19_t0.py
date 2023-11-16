from pymongo import MongoClient
import csv

# MongoDB connection setup
client = MongoClient('mongodb', 27017)
db = client.tpch

# Aggregate pipeline for the MongoDB query
pipeline = [
    {
        '$lookup': {
            'from': 'part',
            'localField': 'L_PARTKEY',
            'foreignField': 'P_PARTKEY',
            'as': 'part'
        }
    },
    {
        '$unwind': '$part'
    },
    {
        '$match': {
            '$or': [
                {
                    'part.P_BRAND': 'Brand#12',
                    'part.P_CONTAINER': {'$in': ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']},
                    'L_QUANTITY': {'$gte': 1, '$lte': 11},
                    'part.P_SIZE': {'$gte': 1, '$lte': 5},
                    'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']},
                    'L_SHIPINSTRUCT': 'DELIVER IN PERSON'
                },
                {
                    'part.P_BRAND': 'Brand#23',
                    'part.P_CONTAINER': {'$in': ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK']},
                    'L_QUANTITY': {'$gte': 10, '$lte': 20},
                    'part.P_SIZE': {'$gte': 1, '$lte': 10},
                    'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']},
                    'L_SHIPINSTRUCT': 'DELIVER IN PERSON'
                },
                {
                    'part.P_BRAND': 'Brand#34',
                    'part.P_CONTAINER': {'$in': ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']},
                    'L_QUANTITY': {'$gte': 20, '$lte': 30},
                    'part.P_SIZE': {'$gte': 1, '$lte': 15},
                    'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']},
                    'L_SHIPINSTRUCT': 'DELIVER IN PERSON'
                }
            ]
        }
    },
    {
        '$group': {
            '_id': None,
            'REVENUE': {
                '$sum': {
                    '$multiply': [
                        '$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}
                    ]
                }
            }
        }
    }
]

# Execute the aggregation pipeline
result = list(db.lineitem.aggregate(pipeline))

# Write the result to a CSV file
with open('query_output.csv', 'w', newline='') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(['REVENUE'])  # Column header
    for data in result:
        writer.writerow([data['REVENUE']])
