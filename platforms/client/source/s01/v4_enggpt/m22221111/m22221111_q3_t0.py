from pymongo import MongoClient
from datetime import datetime
import csv

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client.tpch

# Define the date boundary
date_boundary = datetime(1995, 3, 15)

# Aggregation pipeline to conduct the specified analysis
pipeline = [
    # Join 'customer' with 'orders' based on matching customer keys
    {
        "$lookup": {
            "from": "orders",
            "localField": "C_CUSTKEY",
            "foreignField": "O_CUSTKEY",
            "as": "customer_orders"
        }
    },
    # Unwind the result of the join for further filtering
    {"$unwind": "$customer_orders"},
    # Filter based on market segment and order date
    {
        "$match": {
            "C_MKTSEGMENT": "BUILDING",
            "customer_orders.O_ORDERDATE": {"$lt": date_boundary}
        }
    },
    # Join with 'lineitem' based on matching order keys
    {
        "$lookup": {
            "from": "lineitem",
            "localField": "customer_orders.O_ORDERKEY",
            "foreignField": "L_ORDERKEY",
            "as": "order_lineitems"
        }
    },
    # Unwind the result of the join
    {"$unwind": "$order_lineitems"},
    # Apply final filtering on lineitem ship date
    {
        "$match": {
            "order_lineitems.L_SHIPDATE": {"$gt": date_boundary}
        }
    },
    # Calculate revenue
    {
        "$project": {
            "O_ORDERKEY": "$customer_orders.O_ORDERKEY",
            "O_ORDERDATE": "$customer_orders.O_ORDERDATE",
            "O_SHIPPRIORITY": "$customer_orders.O_SHIPPRIORITY",
            "Revenue": {
                "$subtract": [
                    "$order_lineitems.L_EXTENDEDPRICE",
                    {"$multiply": ["$order_lineitems.L_EXTENDEDPRICE", "$order_lineitems.L_DISCOUNT"]}
                ]
            }
        }
    },
    # Group by keys specified for final analysis
    {
        "$group": {
            "_id": {
                "O_ORDERKEY": "$O_ORDERKEY",
                "O_ORDERDATE": "$O_ORDERDATE",
                "O_SHIPPRIORITY": "$O_SHIPPRIORITY"
            },
            "TotalRevenue": {"$sum": "$Revenue"}
        }
    },
    # Sort results as specified
    {
        "$sort": {
            "TotalRevenue": -1,
            "_id.O_ORDERDATE": 1
        }
    }
]

# Execute the aggregation pipeline
results = list(db.customer.aggregate(pipeline))

# Write the results to the CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY', 'TotalRevenue']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for r in results:
        writer.writerow({
            'O_ORDERKEY': r['_id']['O_ORDERKEY'],
            'O_ORDERDATE': r['_id']['O_ORDERDATE'].strftime("%Y-%m-%d"),
            'O_SHIPPRIORITY': r['_id']['O_SHIPPRIORITY'],
            'TotalRevenue': r['TotalRevenue']
        })

# Close the MongoDB connection
client.close()
