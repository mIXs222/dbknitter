import pymysql
import pymongo
import csv
from datetime import datetime, timedelta

# Connection to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Connection to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Perform the query on MySQL for the revenue0 subquery
mysql_cursor.execute("""
SELECT
    L_SUPPKEY as SUPPLIER_NO,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as TOTAL_REVENUE
FROM
    lineitem
WHERE
    L_SHIPDATE >= '1996-01-01' AND
    L_SHIPDATE < %s
GROUP BY
    L_SUPPKEY
""", (datetime(1996, 1, 1) + timedelta(months=3)).strftime('%Y-%m-%d'))

revenue_data = mysql_cursor.fetchall()

# Find max revenue
max_revenue = max(revenue_data, key=lambda x: x[1])[1] if revenue_data else 0

# Get the supplier details from MongoDB
supplier_data = list(mongodb.supplier.find(
    {'S_SUPPKEY': {'$in': [sup_no for sup_no, rev in revenue_data if rev == max_revenue]}},
    {'_id': 0, 'S_SUPPKEY': 1, 'S_NAME': 1, 'S_ADDRESS': 1, 'S_PHONE': 1}
))

# Join the data
results = [
    {
        "S_SUPPKEY": supplier['S_SUPPKEY'],
        "S_NAME": supplier['S_NAME'],
        "S_ADDRESS": supplier['S_ADDRESS'],
        "S_PHONE": supplier['S_PHONE'],
        "TOTAL_REVENUE": max_revenue
    }
    for supplier in supplier_data
]

# Write results to CVS
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for result in results:
        writer.writerow(result)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
