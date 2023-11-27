# query.py
from pymongo import MongoClient
import csv
from datetime import datetime

# Function to query the MongoDB
def query_mongodb(client, start_date, end_date, ship_modes, priorities):
    db = client.tpch
    
    # Aggregates data to match the SQL-like query
    pipeline = [
        {
            "$match": {
                "L_SHIPMODE": {"$in": ship_modes},
                "L_RECEIPTDATE": {
                    "$gte": datetime.strptime(start_date, "%Y-%m-%d"),
                    "$lt": datetime.strptime(end_date, "%Y-%m-%d")
                },
                "L_SHIPDATE": {"$lt": "$L_COMMITDATE"},
                "L_RECEIPTDATE": {"$gt": "$L_COMMITDATE"}
            }
        },
        {
            "$lookup": {
                "from": "orders",
                "localField": "L_ORDERKEY",
                "foreignField": "O_ORDERKEY",
                "as": "order_info"
            }
        },
        {
            "$unwind": "$order_info"
        },
        {
            "$match": {
                "order_info.O_ORDERPRIORITY": {"$in": priorities}
            }
        },
        {
            "$group": {
                "_id": {
                    "L_SHIPMODE": "$L_SHIPMODE",
                    "O_ORDERPRIORITY": "$order_info.O_ORDERPRIORITY"
                },
                "late_count": {"$sum": 1}
            }
        }
    ]

    return list(db.lineitem.aggregate(pipeline))

# Main function to connect to the database and write results
def main():
    # Connection details
    hostname = "mongodb"
    port = 27017
    client = MongoClient(hostname, port)
    
    # Query parameters
    start_date = "1994-01-01"
    end_date = "1995-01-01"
    ship_modes = ["MAIL", "SHIP"]
    priorities = ["URGENT", "HIGH"]
    
    # Executing the MongoDB query
    results = query_mongodb(client, start_date, end_date, ship_modes, priorities)
    
    # Write results to CSV
    with open('query_output.csv', 'w', newline='') as csvfile:
        fieldnames = ['L_SHIPMODE', 'O_ORDERPRIORITY', 'late_count']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for result in results:
            writer.writerow({
                'L_SHIPMODE': result['_id']['L_SHIPMODE'],
                'O_ORDERPRIORITY': result['_id']['O_ORDERPRIORITY'],
                'late_count': result['late_count']
            })

if __name__ == "__main__":
    main()
