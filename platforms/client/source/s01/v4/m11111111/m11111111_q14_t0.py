from pymongo import MongoClient
import csv
import datetime

# MongoDB connection string
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Select appropriate collections
parts_collection = db['part']
lineitem_collection = db['lineitem']

# Query start and end dates
start_date = datetime.datetime(1995, 9, 1)
end_date = datetime.datetime(1995, 10, 1)

# Aggregation pipeline
pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {
                '$gte': start_date,
                '$lt': end_date
            }
        }
    },
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
        '$project': {
            'PROMO_REVENUE': {
                '$cond': {
                    'if': {'$regexMatch': {'input': '$part.P_TYPE', 'regex': '^PROMO'}},
                    'then': {
                        '$multiply': [
                            '$L_EXTENDEDPRICE',
                            {'$subtract': [1, '$L_DISCOUNT']}
                        ]
                    },
                    'else': 0
                }
            },
            'REVENUE': {
                '$multiply': [
                    '$L_EXTENDEDPRICE',
                    {'$subtract': [1, '$L_DISCOUNT']}
                ]
            }
        }
    },
    {
        '$group': {
            '_id': None,
            'PROMO_REVENUE_TOTAL': {'$sum': '$PROMO_REVENUE'},
            'REVENUE_TOTAL': {'$sum': '$REVENUE'}
        }
    },
    {
        '$project': {
            'PROMO_REVENUE': {
                '$multiply': [{'$divide': ['$PROMO_REVENUE_TOTAL', '$REVENUE_TOTAL']}, 100]
            }
        }
    }
]

result = lineitem_collection.aggregate(pipeline)
promo_revenue_result = list(result)[0]['PROMO_REVENUE']

# Write output to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['PROMO_REVENUE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    writer.writeheader()
    writer.writerow({'PROMO_REVENUE': promo_revenue_result})

print('Query result written to query_output.csv')
