import pymongo
import csv
from datetime import datetime

# Connect to the MongoDB
client = pymongo.MongoClient("mongodb://mongodb:27017/")
db = client['tpch']

# Specify the date range
start_date = datetime(1993, 7, 1)
end_date = datetime(1993, 10, 1)

# Perform aggregation
pipeline = [
    {
        "$lookup": {
            "from": "lineitem",
            "localField": "O_ORDERKEY",
            "foreignField": "L_ORDERKEY",
            "as": "lineitems"
        }
    },
    {
        "$match": {
            "O_ORDERDATE": {"$gte": start_date, "$lt": end_date},
            "lineitems": {"$elemMatch": {"L_COMMITDATE": {"$lt": "$$ROOT.L_RECEIPTDATE"}}}
        }
    },
    {
        "$group": {
            "_id": "$O_ORDERPRIORITY",
            "ORDER_COUNT": {"$sum": 1}
        }
    },
    {
        "$sort": {"_id": 1}
    },
    {
        "$project": {
            "O_ORDERPRIORITY": "$_id",
            "ORDER_COUNT": 1,
            "_id": 0
        }
    }
]

result = list(db.orders.aggregate(pipeline))

# Write the result to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['ORDER_COUNT', 'O_ORDERPRIORITY']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for data in result:
        writer.writerow({'ORDER_COUNT': data['ORDER_COUNT'], 'O_ORDERPRIORITY': data['O_ORDERPRIORITY']})

print("Query results written to 'query_output.csv'")
