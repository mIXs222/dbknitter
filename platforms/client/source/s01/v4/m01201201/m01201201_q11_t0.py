import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
partsupp_collection = mongodb_db['partsupp']

# Fetch data from MySQL
mysql_cursor.execute("SELECT S_SUPPKEY, N_NATIONKEY FROM supplier WHERE S_NATIONKEY IN (SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'GERMANY')")
supplier_data = {row[0]: row[1] for row in mysql_cursor.fetchall()}

# Fetch data from MongoDB
partsupp_cursor = partsupp_collection.find({"PS_SUPPKEY": {"$in": list(supplier_data.keys())}})
partsupp_data = list(partsupp_cursor)

# Compute the total value from MongoDB
total_value = sum(doc['PS_SUPPLYCOST'] * doc['PS_AVAILQTY'] for doc in partsupp_data if doc['PS_SUPPKEY'] in supplier_data)

# Aggregating and grouping data
grouped_data = {}
for doc in partsupp_data:
    key = doc['PS_PARTKEY']
    value = doc['PS_SUPPLYCOST'] * doc['PS_AVAILQTY']
    grouped_data.setdefault(key, 0)
    grouped_data[key] += value

# Filter and sort results
result = {key: val for key, val in grouped_data.items() if val > total_value * 0.0001}
sorted_result = sorted(result.items(), key=lambda item: item[1], reverse=True)

# Write output to CSV file
with open('query_output.csv', 'w', newline='') as file:
    csv_writer = csv.writer(file)
    csv_writer.writerow(['PS_PARTKEY', 'VALUE'])
    for row in sorted_result:
        csv_writer.writerow(row)

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Close MongoDB connection
mongodb_client.close()
