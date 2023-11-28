import pymongo
import csv
from datetime import datetime

# Connect to the MongoDB database
client = pymongo.MongoClient("mongodb", 27017)
db = client["tpch"]
lineitem = db["lineitem"]

# Define the criteria for filtering the line items
start_date = datetime(1994, 1, 1)
end_date = datetime(1994, 12, 31)
min_discount = 0.05
max_discount = 0.07
max_quantity = 24

# Query the lineitem collection based on the defined criteria
pipeline = [
    {
        "$match": {
            "L_SHIPDATE": {"$gte": start_date, "$lte": end_date},
            "L_DISCOUNT": {"$gte": min_discount, "$lte": max_discount},
            "L_QUANTITY": {"$lt": max_quantity}
        }
    },
    {
        "$project": {
            "_id": 0,
            "revenue": {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]},
        }
    },
    {
        "$group": {
            "_id": None,
            "total_revenue": {"$sum": "$revenue"}
        }
    }
]

result = list(lineitem.aggregate(pipeline))

# Write the query's output to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['total_revenue']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    writer.writeheader()
    for data in result:
        writer.writerow({'total_revenue': data['total_revenue']})
