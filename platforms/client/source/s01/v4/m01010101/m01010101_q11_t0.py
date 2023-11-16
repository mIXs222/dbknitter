# sql_and_mongo_query.py
import pymysql
import pymongo
import pandas as pd
import csv
from pymongo import MongoClient

# Connect to MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB database
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
supplier_collection = mongo_db['supplier']

# Get all suppliers from Germany in MongoDB
german_suppliers = list(supplier_collection.find({'S_NATIONKEY': {'$eq': 'GERMANY'}}))
german_supplier_keys = [supplier['S_SUPPKEY'] for supplier in german_suppliers]

# Fetch partsupp and nation data from MySQL database
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT N_NATIONKEY FROM nation WHERE N_NAME='GERMANY'")
    german_nation = cursor.fetchone()
    german_nation_key = german_nation[0]

    cursor.execute("SELECT PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST, PS_AVAILQTY FROM partsupp WHERE PS_SUPPKEY IN (%s)",
                   (','.join(map(str, german_supplier_keys))))
    partsupps = cursor.fetchall()

# Compute the aggregation
aggregation_result = {}
for ps in partsupps:
    if ps[1] not in german_supplier_keys:
        continue
    key = ps[0]
    value = ps[2] * ps[3]  # PS_SUPPLYCOST * PS_AVAILQTY
    if key in aggregation_result:
        aggregation_result[key] += value
    else:
        aggregation_result[key] = value

# Compute the threshold value
threshold_value = sum(aggregation_result.values()) * 0.0001000000

# Filter results based on the threshold
filtered_results = {k: v for k, v in aggregation_result.items() if v > threshold_value}

# Sort the results
sorted_results = sorted(filtered_results.items(), key=lambda item: item[1], reverse=True)

# Write the query output
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['PS_PARTKEY', 'VALUE'])
    for partkey, value in sorted_results:
        writer.writerow([partkey, value])

# Close the connections
mysql_conn.close()
mongo_client.close()
