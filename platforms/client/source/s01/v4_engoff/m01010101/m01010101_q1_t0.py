# execute_query.py
import csv
from pymongo import MongoClient
from datetime import datetime

# Function to connect to the MongoDB database
def connect_to_mongodb(host, port, db_name):
    client = MongoClient(host, port)
    db = client[db_name]
    return db

# Function to execute the query and write the results to a CSV file
def execute_query(db):
    lineitem_collection = db.lineitem

    pipeline = [
        {
            '$match': {
                'L_SHIPDATE': {'$lt': datetime(1998, 9, 2)}
            }
        },
        {
            '$group': {
                '_id': {'L_RETURNFLAG': '$L_RETURNFLAG', 'L_LINESTATUS': '$L_LINESTATUS'},
                'sum_qty': {'$sum': '$L_QUANTITY'},
                'sum_base_price': {'$sum': '$L_EXTENDEDPRICE'},
                'sum_disc_price': {'$sum': {'$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}] }},
                'sum_charge': {'$sum': {'$multiply': [{'$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}]}, {'$add': [1, '$L_TAX']}] }},
                'avg_qty': {'$avg': '$L_QUANTITY'},
                'avg_price': {'$avg': '$L_EXTENDEDPRICE'},
                'avg_disc': {'$avg': '$L_DISCOUNT'},
                'count_order': {'$sum': 1}
            }
        },
        {
            '$sort': {'_id.L_RETURNFLAG': 1, '_id.L_LINESTATUS': 1}
        },
        {
            '$project': {
                'L_RETURNFLAG': '$_id.L_RETURNFLAG',
                'L_LINESTATUS': '$_id.L_LINESTATUS',
                'sum_qty': 1,
                'sum_base_price': 1,
                'sum_disc_price': 1,
                'sum_charge': 1,
                'avg_qty': 1,
                'avg_price': 1,
                'avg_disc': 1,
                'count_order': 1,
                '_id': 0
            }
        }
    ]

    result = list(lineitem_collection.aggregate(pipeline))

    # Write to CSV
    with open('query_output.csv', 'w', newline='') as csvfile:
        fieldnames = ['L_RETURNFLAG', 'L_LINESTATUS', 'sum_qty', 'sum_base_price', 'sum_disc_price', 'sum_charge', 'avg_qty', 'avg_price', 'avg_disc', 'count_order']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for data in result:
            writer.writerow(data)

# Main script
if __name__ == '__main__':
    db = connect_to_mongodb('mongodb', 27017, 'tpch')
    execute_query(db)
