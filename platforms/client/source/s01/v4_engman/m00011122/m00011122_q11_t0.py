import pymysql
import pymongo
import pandas as pd
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
supplier_collection = mongo_db['supplier']
partsupp_collection = mongo_db['partsupp']

# MySQL query to get nation keys for GERMANY
mysql_cursor.execute("SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'GERMANY';")
nation_keys = [row[0] for row in mysql_cursor.fetchall()]

# Get supplier keys for suppliers in GERMANY from MongoDB
supplier_keys = []
for supplier in supplier_collection.find({'S_NATIONKEY': {'$in': nation_keys}}, {'S_SUPPKEY': 1}):
    supplier_keys.append(supplier['S_SUPPKEY'])

# Get parts and calculate total value from partsupp collection in MongoDB
parts = []
total_value = 0.0

for ps in partsupp_collection.find({'PS_SUPPKEY': {'$in': supplier_keys}}):
    ps_value = ps['PS_AVAILQTY'] * ps['PS_SUPPLYCOST']
    total_value += ps_value
    parts.append((ps['PS_PARTKEY'], ps_value))

# Filter parts that represent a significant percentage of total value
significant_parts = filter(lambda x: x[1] / total_value > 0.0001, parts)

# Sort by value in descending order
sorted_parts = sorted(significant_parts, key=lambda x: x[1], reverse=True)

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['Part Number', 'Value'])
    for part in sorted_parts:
        csvwriter.writerow(part)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
