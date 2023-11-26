# File: execute_query.py

import pymysql
import pymongo
import csv

# Connect to MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Perform the subquery to get the average C_ACCTBAL
with mysql_conn.cursor() as mysql_cursor:
    mysql_cursor.execute("""
        SELECT AVG(C_ACCTBAL) AS AVG_ACCTBAL
        FROM customer
        WHERE C_ACCTBAL > 0.00 AND SUBSTR(C_PHONE , 1 , 2) IN 
        ('20', '40', '22', '30', '39', '42', '21')
    """)
    result = mysql_cursor.fetchone()
    avg_acctbal = result[0]

# Perform the main query in MongoDB
pipeline = [
    {
        '$match': {
            'C_ACCTBAL': {'$gt': avg_acctbal},
            'C_PHONE': {'$regex': '^(20|40|22|30|39|42|21)'}
        }
    },
    {
        '$project': {
            'CNTRYCODE': {'$substr': ['$C_PHONE', 0, 2]},
            'C_ACCTBAL': 1,
            '_id': 0
        }
    },
    {
        '$group': {
            '_id': '$CNTRYCODE',
            'NUMCUST': {'$sum': 1},
            'TOTACCTBAL': {'$sum': '$C_ACCTBAL'}
        }
    },
    {
        '$sort': {'_id': 1}
    }
]

customers_no_orders = list(mongodb.customer.aggregate(pipeline))

# Write results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['CNTRYCODE', 'NUMCUST', 'TOTACCTBAL']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for record in customers_no_orders:
        writer.writerow({
            'CNTRYCODE': record['_id'],
            'NUMCUST': record['NUMCUST'],
            'TOTACCTBAL': record['TOTACCTBAL']
        })

# Close connections
mysql_conn.close()
mongo_client.close()
