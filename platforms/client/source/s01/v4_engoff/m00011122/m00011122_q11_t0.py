# File: query_executor.py
import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
supplier_collection = mongo_db['supplier']
partsupp_collection = mongo_db['partsupp']

# Query to get nation keys for Germany from MySQL
mysql_cursor.execute("SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'GERMANY'")
german_nation_keys = [row[0] for row in mysql_cursor.fetchall()]

# Find all suppliers in Germany
german_suppliers = list(supplier_collection.find({'S_NATIONKEY': {'$in': german_nation_keys}}))

# Extract supplier keys
german_supplier_keys = [supplier['S_SUPPKEY'] for supplier in german_suppliers]

# Find all parts supplied by suppliers in Germany
german_partsupplies = list(partsupp_collection.find({'PS_SUPPKEY': {'$in': german_supplier_keys}}))

# Compute the total value and filter significant stocks
significant_parts = []
total_value = sum(part['PS_SUPPLYCOST'] * part['PS_AVAILQTY'] for part in german_partsupplies)

for part in german_partsupplies:
    value = part['PS_SUPPLYCOST'] * part['PS_AVAILQTY']
    if value / total_value > 0.0001:
        significant_parts.append({
            'PS_PARTKEY': part['PS_PARTKEY'],
            'Value': value
        })

# Sort parts by value in descending order
significant_parts.sort(key=lambda x: x['Value'], reverse=True)

# Write output to CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['PS_PARTKEY', 'Value'])
    for part in significant_parts:
        writer.writerow([part['PS_PARTKEY'], part['Value']])

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
