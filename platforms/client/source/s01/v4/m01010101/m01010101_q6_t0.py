from pymongo import MongoClient
import csv

# Connection to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']
lineitem_collection = db['lineitem']

# Convert string date to datetime
from datetime import datetime
start_date = datetime.strptime('1994-01-01', '%Y-%m-%d')
end_date = datetime.strptime('1995-01-01', '%Y-%m-%d')

# Querying MongoDB
pipeline = [
    {
        "$match": {
            "L_SHIPDATE": {
                "$gte": start_date,
                "$lt": end_date
            },
            "L_DISCOUNT": {
                "$gte": 0.05,
                "$lte": 0.07
            },
            "L_QUANTITY": {
                "$lt": 24
            }
        }
    },
    {
        "$group": {
            "_id": None,
            "REVENUE": {
                "$sum": {
                    "$multiply": ["$L_EXTENDEDPRICE", "$L_DISCOUNT"]
                }
            }
        }
    },
    {
        "$project": {
            "_id": 0,
            "REVENUE": 1
        }
    }
]

result = list(lineitem_collection.aggregate(pipeline))

# Writing results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['REVENUE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for data in result:
        writer.writerow(data)
