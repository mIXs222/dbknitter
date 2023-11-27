# mongo_query.py

from pymongo import MongoClient
import csv
from datetime import datetime

# Mongo Connection Information
mongo_host = "mongodb"
mongo_port = 27017
mongo_db_name = "tpch"

# Connect to MongoDB
client = MongoClient(mongo_host, mongo_port)
db = client[mongo_db_name]

# Dates for the query
start_date = datetime(1993, 10, 1)
end_date = datetime(1994, 1, 1)

# Perform aggregation query
pipeline = [
    {
        "$match": {
            "L_SHIPDATE": {"$gte": start_date, "$lt": end_date},
            "L_RETURNFLAG": "R"
        }
    },
    {
        "$lookup": {
            "from": "orders",
            "localField": "L_ORDERKEY",
            "foreignField": "O_ORDERKEY",
            "as": "order"
        }
    },
    {"$unwind": "$order"},
    {
        "$lookup": {
            "from": "customer",
            "localField": "order.O_CUSTKEY",
            "foreignField": "C_CUSTKEY",
            "as": "customer"
        }
    },
    {"$unwind": "$customer"},
    {
        "$lookup": {
            "from": "nation",
            "localField": "customer.C_NATIONKEY",
            "foreignField": "N_NATIONKEY",
            "as": "nation"
        }
    },
    {"$unwind": "$nation"},
    {
        "$group": {
            "_id": {
                "C_CUSTKEY": "$customer.C_CUSTKEY",
                "C_NAME": "$customer.C_NAME",
                "C_ADDRESS": "$customer.C_ADDRESS",
                "N_NAME": "$nation.N_NAME",
                "C_PHONE": "$customer.C_PHONE",
                "C_ACCTBAL": "$customer.C_ACCTBAL",
                "C_COMMENT": "$customer.C_COMMENT"
            },
            "REVENUE_LOST": {
                "$sum": {
                    "$multiply": [
                        "$L_EXTENDEDPRICE",
                        {"$subtract": [1, "$L_DISCOUNT"]}
                    ]
                }
            }
        }
    },
    {
        "$project": {
            "C_CUSTKEY": "$_id.C_CUSTKEY",
            "C_NAME": "$_id.C_NAME",
            "C_ADDRESS": "$_id.C_ADDRESS",
            "N_NAME": "$_id.N_NAME",
            "C_PHONE": "$_id.C_PHONE",
            "C_ACCTBAL": "$_id.C_ACCTBAL",
            "C_COMMENT": "$_id.C_COMMENT",
            "REVENUE_LOST": "$REVENUE_LOST",
            "_id": 0
        }
    },
    {
        "$sort": {
            "REVENUE_LOST": -1,
            "C_CUSTKEY": 1,
            "C_NAME": 1,
            "C_ACCTBAL": 1
        }
    }
]

results = list(db.lineitem.aggregate(pipeline))

# Write results to CSV
with open("query_output.csv", mode="w", newline="") as file:
    writer = csv.writer(file)
    
    # Write header
    writer.writerow([
        "CUSTOMER_KEY", "CUSTOMER_NAME",
        "ADDRESS", "NATION", "PHONE", "ACCTBAL",
        "COMMENT", "REVENUE_LOST"
    ])
    
    # Write rows
    for row in results:
        writer.writerow([
            row["C_CUSTKEY"], row["C_NAME"],
            row["C_ADDRESS"], row["N_NAME"], row["C_PHONE"],
            row["C_ACCTBAL"], row["C_COMMENT"], row["REVENUE_LOST"]
        ])
