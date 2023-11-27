from pymongo import MongoClient
from datetime import datetime
import csv

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# MongoDB collections
part_coll = db['part']
lineitem_coll = db['lineitem']

# Date range for query
start_date = datetime(1995, 9, 1)
end_date = datetime(1995, 10, 1)

# Aggregation pipeline for the query
pipeline = [
    {
        "$match": {
            "L_SHIPDATE": {"$gte": start_date, "$lt": end_date}
        }
    },
    {
        "$lookup": {
            "from": "part",
            "localField": "L_PARTKEY",
            "foreignField": "P_PARTKEY",
            "as": "part_details"
        }
    },
    {
        "$unwind": "$part_details"
    },
    {
        "$project": {
            "revenue": {
                "$multiply": [
                    "$L_EXTENDEDPRICE",
                    {"$subtract": [1, "$L_DISCOUNT"]}
                ]
            },
            "P_NAME": "$part_details.P_NAME"
        }
    },
    {
        "$group": {
            "_id": None,
            "total_revenue": {"$sum": "$revenue"},
            "promo_revenue": {"$sum": {
                "$cond": [
                    {"$in": ["PROMO", "$P_NAME"]},
                    "$revenue",
                    0
                ]
            }}
        }
    }
]

# Execute the aggregation pipeline
results = list(lineitem_coll.aggregate(pipeline))

# Calculate the percentage of promo revenue
if results and 'total_revenue' in results[0] and results[0]['total_revenue'] != 0:
    percentage = (results[0]['promo_revenue'] / results[0]['total_revenue']) * 100
else:
    percentage = 0

# Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['percentage']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerow({'percentage': percentage})

# Close the MongoDB connection
client.close()
