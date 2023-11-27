from pymongo import MongoClient
import csv

# Connecting to the MongoDB database
client = MongoClient('mongodb', 27017)
db = client.tpch
lineitem_collection = db.lineitem

pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {
                '$gte': '1994-01-01',
                '$lt': '1995-01-01'
            },
            'L_DISCOUNT': {
                '$gte': 0.05,
                '$lte': 0.07
            },
            'L_QUANTITY': {
                '$lt': 24
            }
        }
    },
    {
        '$project': {
            'REVENUE': {
                '$multiply': ['$L_EXTENDEDPRICE', '$L_DISCOUNT']
            }
        }
    },
    {
        '$group': {
            '_id': None,
            'TOTAL_REVENUE': {
                '$sum': '$REVENUE'
            }
        }
    }
]

result = list(lineitem_collection.aggregate(pipeline))

# Write the output to the file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['REVENUE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for data in result:
        writer.writerow({'REVENUE': data['TOTAL_REVENUE']})
