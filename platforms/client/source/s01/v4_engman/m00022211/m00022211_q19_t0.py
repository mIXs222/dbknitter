import pymysql
import pymongo
import pandas as pd
import csv

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
mongodb_lineitem = mongodb_db['lineitem']

# Query to select the data from MySQL's part table
mysql_query = """
SELECT P_PARTKEY FROM part
WHERE
    (P_BRAND = 'Brand#12' AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND P_SIZE BETWEEN 1 AND 5) OR
    (P_BRAND = 'Brand#23' AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND P_SIZE BETWEEN 1 AND 10) OR
    (P_BRAND = 'Brand#34' AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND P_SIZE BETWEEN 1 AND 15)
"""
mysql_cursor.execute(mysql_query)
valid_partkeys = [row[0] for row in mysql_cursor.fetchall()]

# MongoDB query to calculate the revenue
pipeline = [
    {
        "$match": {
            "L_PARTKEY": {"$in": valid_partkeys},
            "L_SHIPMODE": {"$in": ["AIR", "AIR REG"]},
            "L_SHIPINSTRUCT": "DELIVER IN PERSON",
            "$or": [
                {"$and": [
                    {"L_QUANTITY": {"$gte": 1, "$lte": 11}},
                    {"L_EXTENDEDPRICE": {"$ne": None}},
                    {"L_DISCOUNT": {"$ne": None}}
                ]},
                {"$and": [
                    {"L_QUANTITY": {"$gte": 10, "$lte": 20}},
                    {"L_EXTENDEDPRICE": {"$ne": None}},
                    {"L_DISCOUNT": {"$ne": None}}
                ]},
                {"$and": [
                    {"L_QUANTITY": {"$gte": 20, "$lte": 30}},
                    {"L_EXTENDEDPRICE": {"$ne": None}},
                    {"L_DISCOUNT": {"$ne": None}}
                ]}
            ]
        }
    },
    {
        "$group": {
            "_id": None,
            "REVENUE": {
                "$sum": {
                    "$multiply": [
                        "$L_EXTENDEDPRICE",
                        {"$subtract": [1, "$L_DISCOUNT"]}
                    ]
                }
            }
        }
    }
]
revenue_result = list(mongodb_lineitem.aggregate(pipeline))

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()

# Write result to CSV file
with open('query_output.csv', mode='w') as file:
    csv_writer = csv.writer(file)
    csv_writer.writerow(['REVENUE'])
    revenue = revenue_result[0]['REVENUE'] if revenue_result else 0
    csv_writer.writerow([revenue])
