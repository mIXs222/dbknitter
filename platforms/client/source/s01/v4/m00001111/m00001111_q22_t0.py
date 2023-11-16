from pymongo import MongoClient
import csv

# MongoDB connection setup
client = MongoClient('mongodb', 27017)
db = client['tpch']
customers = db['customer']
orders = db['orders']

# Helper function to calculate average account balance
def calculate_avg_acct_balance():
    match_stage = {
        "$match": {
            "C_ACCTBAL": {"$gt": 0.00},
            "C_PHONE": {"$regex": r'^(20|40|22|30|39|42|21)'}
        }
    }
    group_stage = {
        "$group": {
            "_id": None,
            "average_balance": {"$avg": "$C_ACCTBAL"}
        }
    }
    result = list(customers.aggregate([match_stage, group_stage]))
    return result[0]['average_balance'] if result else 0

# Calculate average account balance
average_balance = calculate_avg_acct_balance()

# Aggregation pipeline to get the desired output
pipeline = [
    {"$match": {
        "C_PHONE": {"$regex": r'^(20|40|22|30|39|42|21)'},
        "C_ACCTBAL": {"$gt": average_balance}
    }},
    {"$lookup": {
        "from": "orders",
        "localField": "C_CUSTKEY",
        "foreignField": "O_CUSTKEY",
        "as": "customer_orders"
    }},
    {"$match": {"customer_orders": {"$size": 0}}},
    {"$project": {
        "CNTRYCODE": {"$substr": ["$C_PHONE", 0, 2]},
        "C_ACCTBAL": 1,
        "_id": 0
    }},
    {"$group": {
        "_id": "$CNTRYCODE",
        "NUMCUST": {"$sum": 1},
        "TOTACCTBAL": {"$sum": "$C_ACCTBAL"}
    }},
    {"$sort": {"_id": 1}}
]

# Execute the aggregation pipeline
results = customers.aggregate(pipeline)

# Open the CSV file and write the output
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['CNTRYCODE', 'NUMCUST', 'TOTACCTBAL']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    # Write header
    writer.writeheader()

    # Write data rows
    for result in results:
        writer.writerow({
            'CNTRYCODE': result['_id'],
            'NUMCUST': result['NUMCUST'],
            'TOTACCTBAL': result['TOTACCTBAL']
        })
