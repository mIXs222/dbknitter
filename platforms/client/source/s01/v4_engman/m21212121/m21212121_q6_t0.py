from pymongo import MongoClient
import csv
import datetime

# MongoDB connection setup
client = MongoClient('mongodb', 27017)
db = client['tpch']
lineitem_collection = db['lineitem']

# Define the date range and discount range for the query
start_date = datetime.datetime(1994, 1, 2, 0, 0)
end_date = datetime.datetime(1995, 1, 1, 0, 0)
discount_floor = 0.05
discount_ceiling = 0.07

# Query the lineitem collection in MongoDB
pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {'$gt': start_date, '$lt': end_date},
            'L_DISCOUNT': {'$gte': discount_floor, '$lte': discount_ceiling},
            'L_QUANTITY': {'$lt': 24}
        }
    },
    {
        '$group': {
            '_id': None,
            'REVENUE': {
                '$sum': {
                    '$multiply': ['$L_EXTENDEDPRICE', '$L_DISCOUNT']
                }
            }
        }
    },
    {
        '$project': {
            '_id': 0,
            'REVENUE': 1
        }
    }
]

results = list(lineitem_collection.aggregate(pipeline))

# Write the output to query_output.csv
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['REVENUE'])  # Header
    for result in results:
        writer.writerow([result['REVENUE']])

print("Query results written to query_output.csv")
client.close()
