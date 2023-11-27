# forecasting_revenue_change.py
from pymongo import MongoClient
import csv
from datetime import datetime

# MongoDB connection info
DB_NAME = "tpch"
PORT = 27017
HOSTNAME = "mongodb"

# Connect to the MongoDB server
client = MongoClient(HOSTNAME, PORT)
db = client[DB_NAME]


def calculate_potential_revenue_increase():
    # MongoDB Query
    pipeline = [
        {
            "$match": {
                "L_SHIPDATE": {"$gte": datetime(1994, 1, 1), "$lt": datetime(1995, 1, 1)},
                "L_DISCOUNT": {"$gte": 0.06 - 0.01, "$lte": 0.06 + 0.01},
                "L_QUANTITY": {"$lt": 24},
            }
        },
        {
            "$project": {
                "revenue_increase": {
                    "$multiply": ["$L_EXTENDEDPRICE", "$L_DISCOUNT"]
                }
            }
        },
        {
            "$group": {
                "_id": None,
                "total_revenue_increase": {"$sum": "$revenue_increase"}
            }
        }
    ]
    result = list(db.lineitem.aggregate(pipeline))
    total_revenue_increase = (result[0]["total_revenue_increase"]
                              if result else 0)
    
    # Write result to CSV
    with open('query_output.csv', 'w', newline='') as csvfile:
        fieldnames = ['total_revenue_increase']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerow({'total_revenue_increase': total_revenue_increase})


if __name__ == "__main__":
    calculate_potential_revenue_increase()
