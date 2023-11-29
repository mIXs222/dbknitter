import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
lineitem_collection = mongo_db['lineitem']

# Get supplier data from MySQL
suppliers = {}
query_mysql = "SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE FROM supplier"
mysql_cursor.execute(query_mysql)
for S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE in mysql_cursor.fetchall():
    suppliers[S_SUPPKEY] = {'S_NAME': S_NAME, 'S_ADDRESS': S_ADDRESS, 'S_PHONE': S_PHONE, 'TOTAL_REVENUE': 0.0}

# Get lineitem data from MongoDB and calculate revenue
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 4, 1)
lineitems = lineitem_collection.find({
    'L_SHIPDATE': {'$gte': start_date, '$lt': end_date}
})

for lineitem in lineitems:
    revenue = lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])
    if lineitem['L_SUPPKEY'] in suppliers:
        suppliers[lineitem['L_SUPPKEY']]['TOTAL_REVENUE'] += revenue

# Calculate max revenue and filter suppliers
max_revenue = max(supplier['TOTAL_REVENUE'] for supplier in suppliers.values())
top_suppliers = [supplier for supplier in suppliers.values() if supplier['TOTAL_REVENUE'] == max_revenue]

# Sort suppliers by SUPPKEY
top_suppliers_sorted = sorted(top_suppliers, key=lambda x: x['S_PHONE'])

# Write output to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for supplier in top_suppliers_sorted:
        row = {
            'S_SUPPKEY': supplier['S_SUPPKEY'],
            'S_NAME': supplier['S_NAME'],
            'S_ADDRESS': supplier['S_ADDRESS'],
            'S_PHONE': supplier['S_PHONE'],
            'TOTAL_REVENUE': round(supplier['TOTAL_REVENUE'], 2)
        }
        writer.writerow(row)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
