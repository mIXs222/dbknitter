from pymongo import MongoClient
import csv

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Fetch data from MongoDB
orders_collection = db.orders
lineitem_collection = db.lineitem

# Define the pipeline for the aggregation
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
        "$unwind": "$lineitems"
    },
    {
        "$match": {
            "O_ORDERDATE": {"$gte": "1993-07-01", "$lt": "1993-10-01"},
            "lineitems.L_COMMITDATE": {"$lt": "lineitems.L_RECEIPTDATE"}
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
    }
]

# Execute aggregation
results = orders_collection.aggregate(pipeline)

# Write results to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['ORDER_COUNT', 'O_ORDERPRIORITY']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for result in results:
        writer.writerow({'ORDER_COUNT': result['ORDER_COUNT'], 'O_ORDERPRIORITY': result['_id']})

# Close the connection to the MongoDB client
client.close()
