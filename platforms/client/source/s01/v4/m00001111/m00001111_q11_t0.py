# multi_db_query.py
import pymysql
import pymongo
import csv
import os

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.Cursor
)

# Connect to MongoDB
mongodb_client = pymongo.MongoClient("mongodb", 27017)
mongodb_db = mongodb_client['tpch']
partsupp_collection = mongodb_db['partsupp']

# Query MongoDB to get the partsupp entries related to Germany
mongo_query = [
    {'$lookup': {
        'from': 'supplier',
        'localField': 'PS_SUPPKEY',
        'foreignField': 'S_SUPPKEY',
        'as': 'supplier_data'
    }},
    {'$unwind': '$supplier_data'},
    {'$match': {'supplier_data.S_NATIONKEY': {'$in': []}}}
]

# Placeholder for nation keys related to Germany (will be fetched from MySQL)
germany_nation_keys = []

# Execute query on MySQL to get the nation keys for Germany
with mysql_connection.cursor() as cursor:
    cursor.execute(
        "SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'GERMANY'"
    )
    for row in cursor.fetchall():
        germany_nation_keys.append(row[0])

# Update Mongo query with actual nation keys
mongo_query[2]['$match']['supplier_data.S_NATIONKEY']['$in'] = germany_nation_keys

# Execute adjusted MongoDB query
partsupp_entries = list(partsupp_collection.aggregate(mongo_query))

# Calculate SUM(PS_SUPPLYCOST * PS_AVAILQTY) for each part key and filter accordingly
part_values = {}
for entry in partsupp_entries:
    key = entry['PS_PARTKEY']
    value = entry['PS_SUPPLYCOST'] * entry['PS_AVAILQTY']
    part_values[key] = part_values.get(key, 0) + value

# Calculate the threshold from MySQL part
with mysql_connection.cursor() as cursor:
    cursor.execute(
        "SELECT SUM(PS_SUPPLYCOST * PS_AVAILQTY) * 0.0001000000 FROM partsupp, supplier, nation "
        "WHERE PS_SUPPKEY = S_SUPPKEY AND S_NATIONKEY = N_NATIONKEY AND N_NAME = 'GERMANY'"
    )
    threshold = cursor.fetchone()[0]

# Filter part_values based on threshold
filtered_results = {k: v for k, v in part_values.items() if v > threshold}

# Sort the filtered results
sorted_results = sorted(filtered_results.items(), key=lambda item: item[1], reverse=True)

# Write results to CSV file
output_filename = 'query_output.csv'
with open(output_filename, 'w', newline='') as csvfile:
    fieldnames = ['PS_PARTKEY', 'VALUE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for partkey, value in sorted_results:
        writer.writerow({'PS_PARTKEY': partkey, 'VALUE': value})

# Close the connections
mysql_connection.close()
mongodb_client.close()
