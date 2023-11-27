import pymongo
import csv
from datetime import datetime

# Constants
MONGODB_HOST = "mongodb"
MONGODB_PORT = 27017
MONGODB_DB_NAME = "tpch"

# Open CSV file for writing output
with open('query_output.csv', mode='w') as output_file:
    output_writer = csv.writer(output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

    # Connecting to MongoDB
    client = pymongo.MongoClient(host=MONGODB_HOST, port=MONGODB_PORT)
    db = client[MONGODB_DB_NAME]

    # MongoDB query
    start_date = datetime(1994, 1, 1)
    end_date = datetime(1995, 1, 1)
    min_discount = 0.05
    max_discount = 0.07

    pipeline = [
        {
            '$match': {
                'L_SHIPDATE': {
                    '$gte': start_date,
                    '$lt': end_date
                },
                'L_DISCOUNT': {
                    '$gte': min_discount,
                    '$lte': max_discount
                },
                'L_QUANTITY': {
                    '$lt': 24
                }
            }
        },
        {
            '$project': {
                'revenue_increase': {
                    '$multiply': [
                        '$L_EXTENDEDPRICE', '$L_DISCOUNT'
                    ]
                }
            }
        },
        {
            '$group': {
                '_id': None,
                'total_revenue_increase': { '$sum': '$revenue_increase' }
            }
        }
    ]

    results = db.lineitem.aggregate(pipeline)

    # Write results to CSV file
    for result in results:
        output_writer.writerow(['total_revenue_increase', result['total_revenue_increase']])
