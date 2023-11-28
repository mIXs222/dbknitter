from pymongo import MongoClient
import csv
from datetime import datetime

# Connection to MongoDB
client = MongoClient("mongodb", 27017)
db = client['tpch']
lineitem_collection = db['lineitem']

# Query parameters
start_date = datetime.strptime("1994-01-01", "%Y-%m-%d")
end_date = datetime.strptime("1994-12-31", "%Y-%m-%d")
discount_min = 0.06 - 0.01
discount_max = 0.06 + 0.01
max_quantity = 24

# Query execution
query_result = lineitem_collection.aggregate([
    {
        "$match": {
            "L_SHIPDATE": {"$gte": start_date, "$lte": end_date},
            "L_DISCOUNT": {"$gte": discount_min, "$lte": discount_max},
            "L_QUANTITY": {"$lt": max_quantity}
        }
    },
    {
        "$project": {
            "revenue": {
                "$subtract": [
                    "$L_EXTENDEDPRICE",
                    {"$multiply": ["$L_EXTENDEDPRICE", "$L_DISCOUNT"]}
                ]
            }
        }
    },
    {
        "$group": {
            "_id": None,
            "total_revenue": {"$sum": "$revenue"}
        }
    }
])

# Save the query output to CSV
with open('query_output.csv', mode='w') as csv_file:
    fieldnames = ['total_revenue']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    writer.writeheader()
    for data in query_result:
        writer.writerow({'total_revenue': data['total_revenue']})

# Close MongoDB connection
client.close()
