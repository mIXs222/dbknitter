from pymongo import MongoClient
import csv
from datetime import datetime

# Function to connect to MongoDB
def get_mongodb_collection(host, port, database_name, collection_name):
    client = MongoClient(host, port)
    db = client[database_name]
    collection = db[collection_name]
    return collection

# Function to perform the aggregation in MongoDB
def aggregate_data(collection):
    pipeline = [
        {"$match": {
            "L_SHIPDATE": {"$lte": datetime.strptime("1998-09-02", "%Y-%m-%d")}
        }},
        {"$group": {
            "_id": {"L_RETURNFLAG": "$L_RETURNFLAG", "L_LINESTATUS": "$L_LINESTATUS"},
            "SUM_QTY": {"$sum": "$L_QUANTITY"},
            "SUM_BASE_PRICE": {"$sum": "$L_EXTENDEDPRICE"},
            "SUM_DISC_PRICE": {
                "$sum": {
                    "$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]
                }
            },
            "SUM_CHARGE": {
                "$sum": {
                    "$multiply": [
                        "$L_EXTENDEDPRICE",
                        {"$subtract": [1, "$L_DISCOUNT"]},
                        {"$add": [1, "$L_TAX"]}
                    ]
                }
            },
            "AVG_QTY": {"$avg": "$L_QUANTITY"},
            "AVG_PRICE": {"$avg": "$L_EXTENDEDPRICE"},
            "AVG_DISC": {"$avg": "$L_DISCOUNT"},
            "COUNT_ORDER": {"$sum": 1}
        }},
        {"$sort": {
            "_id.L_RETURNFLAG": 1,
            "_id.L_LINESTATUS": 1
        }}
    ]
    return list(collection.aggregate(pipeline))

# Function to write query results to a CSV file
def write_to_csv(data, filename):
    with open(filename, mode='w') as file:
        writer = csv.writer(file)
        # Write header
        writer.writerow([
            'L_RETURNFLAG', 'L_LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE',
            'SUM_DISC_PRICE', 'SUM_CHARGE', 'AVG_QTY', 'AVG_PRICE', 'AVG_DISC',
            'COUNT_ORDER'
        ])
        # Write data rows
        for row in data:
            writer.writerow([
                row['_id']['L_RETURNFLAG'], 
                row['_id']['L_LINESTATUS'],
                row['SUM_QTY'],
                row['SUM_BASE_PRICE'],
                row['SUM_DISC_PRICE'],
                row['SUM_CHARGE'],
                row['AVG_QTY'],
                row['AVG_PRICE'],
                row['AVG_DISC'],
                row['COUNT_ORDER']
            ])

# Main logic
if __name__ == "__main__":
    collection = get_mongodb_collection(
        host="mongodb",
        port=27017,
        database_name="tpch",
        collection_name="lineitem"
    )
    result = aggregate_data(collection)
    write_to_csv(result, 'query_output.csv')
