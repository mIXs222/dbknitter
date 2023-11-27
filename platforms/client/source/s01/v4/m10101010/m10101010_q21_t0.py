# Python Code: query_code.py

import csv
import pymysql
from pymongo import MongoClient
from datetime import datetime

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Execute the MySQL part of the query
mysql_cursor = mysql_connection.cursor()
mysql_cursor.execute("""
SELECT
    supplier.S_SUPPKEY,
    supplier.S_NAME
FROM
    supplier
JOIN
    nation ON supplier.S_NATIONKEY = nation.N_NATIONKEY
WHERE
    nation.N_NAME = 'SAUDI ARABIA'
""")

suppliers_dict = {row[0]: row[1] for row in mysql_cursor.fetchall()}

# Close the MySQL connection
mysql_cursor.close()
mysql_connection.close()

# Execute MongoDB part of the query
orders = mongo_db['orders'].find({'O_ORDERSTATUS': 'F'})
orders_dict = {order['O_ORDERKEY']: order for order in orders}

lineitem_entries = mongo_db['lineitem'].aggregate([
    {
        "$match": {
            "L_RECEIPTDATE": {"$gt": "$L_COMMITDATE"},
            "L_SUPPKEY": {"$in": list(suppliers_dict.keys())}
        }
    },
    {
        "$lookup": {
            "from": "lineitem",
            "localField": "L_ORDERKEY",
            "foreignField": "L_ORDERKEY",
            "as": "other_lineitems"
        }
    },
    {
        "$project": {
            "L_ORDERKEY": 1,
            "L_SUPPKEY": 1,
            "other_supplier_exists": {
                "$anyElementTrue": {
                    "$map": {
                        "input": "$other_lineitems",
                        "as": "item",
                        "in": {
                            "$and": [
                                {"$ne": ["$$item.L_SUPPKEY", "$L_SUPPKEY"]},
                                {"$gt": ["$$item.L_RECEIPTDATE", "$$item.L_COMMITDATE"]}
                            ]
                        }
                    }
                }
            }
        }
    },
    {"$match": {"other_supplier_exists": False}}
])

# Process and collect results
results = {}
for entry in lineitem_entries:
    if entry['L_ORDERKEY'] in orders_dict:
        supplier_name = suppliers_dict[entry['L_SUPPKEY']]
        if supplier_name not in results:
            results[supplier_name] = 0
        results[supplier_name] += 1

# Prepare final results
final_results = sorted(results.items(), key=lambda x: (-x[1], x[0]))

# Write results to file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['S_NAME', 'NUMWAIT'])
    for result in final_results:
        writer.writerow(result)
