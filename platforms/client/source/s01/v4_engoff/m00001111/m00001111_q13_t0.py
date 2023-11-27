from pymongo import MongoClient
import csv

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Prepare the aggregation pipeline
pipeline = [
    {
        "$lookup": {
            "from": "orders",
            "localField": "C_CUSTKEY",
            "foreignField": "O_CUSTKEY",
            "as": "orders"
        }
    },
    {
        "$project": {
            "C_CUSTKEY": 1,
            "orders": {
                "$filter": {
                    "input": "$orders",
                    "as": "order",
                    "cond": {
                        "$and": [
                            {"$ne": ["$$order.O_ORDERSTATUS", "pending"]},
                            {"$ne": ["$$order.O_COMMENT", {"$regex": "(?i)(^|.*\\W)pending($|\\W.*)" }]},
                            {"$ne": ["$$order.O_COMMENT", {"$regex": "(?i)(^|.*\\W)deposits($|\\W.*)" }]}
                        ]
                    }
                }
            }
        }
    },
    {
        "$project": {
            "order_count": {"$size": "$orders"}
        }
    },
    {
        "$group": {
            "_id": "$order_count",
            "num_customers": {"$sum": 1}
        }
    },
    {
        "$sort": {"_id": 1}
    }
]

# Execute the aggregation
results = list(db.customer.aggregate(pipeline))

# Write the query results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['_id', 'num_customers']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for result in results:
        writer.writerow({'_id': result['_id'], 'num_customers': result['num_customers']})

# Close the client
client.close()
