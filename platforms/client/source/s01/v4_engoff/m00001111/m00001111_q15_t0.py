import pymysql
import pymongo
import csv
from datetime import datetime

# Constants
START_DATE = datetime(1996, 1, 1)
END_DATE = datetime(1996, 4, 1)

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
lineitem_collection = mongo_db['lineitem']

# Fetch data from MongoDB
mongo_query = {
    'L_SHIPDATE': {'$gte': START_DATE.strftime("%Y-%m-%d"),
                   '$lt': END_DATE.strftime("%Y-%m-%d")}
}
lineitems_cursor = lineitem_collection.find(mongo_query, {'_id': False})

# Accrue total revenue per supplier based on MongoDB data
supplier_revenue = {}
for lineitem in lineitems_cursor:
    supplier_revenue[lineitem['L_SUPPKEY']] = supplier_revenue.get(lineitem['L_SUPPKEY'], 0) + \
                                              (lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT']))

# Close MongoDB connection
mongo_client.close()

# Query to fetch supplier details from MySQL
supplier_query = 'SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE, S_ACCTBAL, S_COMMENT FROM supplier'

# Execute MySQL query
with mysql_conn.cursor() as cursor:
    cursor.execute(supplier_query)
    suppliers = cursor.fetchall()

# Close MySQL connection
mysql_conn.close()

# Write results to CSV file
max_revenue = max(supplier_revenue.values())
top_suppliers = [(s[0], s[1], s[2], s[3], s[4], s[5], s[6], supplier_revenue.get(s[0], 0))
                 for s in suppliers
                 if supplier_revenue.get(s[0], 0) == max_revenue]

# Sort suppliers by supplier number order
top_suppliers_sorted = sorted(top_suppliers, key=lambda x: x[0])

# Write results to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT', 'REVENUE'])
    for supplier in top_suppliers_sorted:
        writer.writerow(supplier)
