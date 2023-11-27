# query.py

import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
orders_col = mongodb_db['orders']

# Get customers who do not have orders from MongoDB
no_order_custkeys = [doc['O_CUSTKEY'] for doc in orders_col.find()]
no_order_custkeys = set(no_order_custkeys)

# Get avg C_ACCTBAL value only from customers matching the country codes and C_ACCTBAL > 0
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT AVG(C_ACCTBAL) AS AVG_ACCTBAL
        FROM customer
        WHERE C_ACCTBAL > 0.00
        AND SUBSTR(C_PHONE, 1, 2) IN ('20', '40', '22', '30', '39', '42', '21')
    """)
    avg_acctbal = cursor.fetchone()[0]

# Get the country codes, count, and total balance from MySQL customers based on the criteria
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT SUBSTR(C_PHONE, 1, 2) AS CNTRYCODE, COUNT(*) AS NUMCUST, SUM(C_ACCTBAL) AS TOTACCTBAL
        FROM customer
        WHERE SUBSTR(C_PHONE, 1, 2) IN ('20', '40', '22', '30', '39', '42', '21')
        AND C_ACCTBAL > %s AND C_CUSTKEY NOT IN %s
        GROUP BY SUBSTR(C_PHONE, 1, 2)
        ORDER BY CNTRYCODE
    """, (avg_acctbal, tuple(no_order_custkeys),))
    results = cursor.fetchall()

# Write output to file query_output.csv
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    # Write Header
    csvwriter.writerow(['CNTRYCODE', 'NUMCUST', 'TOTACCTBAL'])
    # Write data
    for row in results:
        csvwriter.writerow(row)

# Close database connections
mysql_conn.close()
mongodb_client.close()
