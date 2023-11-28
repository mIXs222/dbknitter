# python_code.py

import pymysql
import pymongo
import csv

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
customer_collection = mongo_db['customer']

# Get the average account balance from MongoDB for specifc country codes
country_codes = ['20', '40', '22', '30', '39', '42', '21']
pipeline = [
    {"$addFields": {"CNTRYCODE": {"$substr": ["$C_PHONE", 0, 2]}}},
    {"$match": {"CNTRYCODE": {"$in": country_codes}, "C_ACCTBAL": {"$gt": 0}}},
    {"$group": {"_id": "$CNTRYCODE", "AVG_ACCTBAL": {"$avg": "$C_ACCTBAL"}}}
]
code_to_avg_bal = {doc['_id']: doc['AVG_ACCTBAL'] for doc in customer_collection.aggregate(pipeline)}

# Get order keys from orders in MySQL to exclude these customers later
with mysql_conn.cursor() as cur:
    cur.execute("SELECT DISTINCT O_CUSTKEY FROM orders")
    exclude_custkeys = [row[0] for row in cur.fetchall()]

# Prepare the final complex query for MongoDB
pipeline = [
    {
        "$addFields": {
            "CNTRYCODE": {"$substr": ["$C_PHONE", 0, 2]}
        }
    },
    {
        "$match": {
            "CNTRYCODE": {"$in": country_codes},
            "C_ACCTBAL": {"$gt": 0},
            "C_CUSTKEY": {"$nin": exclude_custkeys}
        }
    },
    {
        "$group": {
            "_id": "$CNTRYCODE",
            "NUMCUST": {"$sum": 1},
            "TOTACCTBAL": {"$sum": "$C_ACCTBAL"}
        }
    },
    {
        "$project": {
            "CNTRYCODE": "$_id",
            "NUMCUST": 1,
            "TOTACCTBAL": 1,
            "_id": 0
        }
    },
    {"$sort": {"CNTRYCODE": 1}}
]

# Query MongoDB and write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['CNTRYCODE', 'NUMCUST', 'TOTACCTBAL']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for record in customer_collection.aggregate(pipeline):
        # skip customer if account balance is not greater than the average
        if record['TOTACCTBAL'] <= code_to_avg_bal.get(record['CNTRYCODE'], 0):
            continue
        writer.writerow(record)

# Close connections
mysql_conn.close()
mongo_client.close()
