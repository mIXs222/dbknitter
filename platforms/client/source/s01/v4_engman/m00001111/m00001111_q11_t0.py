import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_col = mongo_db['partsupp']

# SQL query to get suppliers from GERMANY
mysql_cursor.execute("SELECT S_SUPPKEY FROM supplier WHERE S_NATIONKEY IN "
                     "(SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'GERMANY');")
suppliers_in_germany = [row[0] for row in mysql_cursor.fetchall()]

# Query MongoDB for partsupp information based on the suppliers fetched from MySQL
partsupp_data = mongo_col.find({'PS_SUPPKEY': {'$in': suppliers_in_germany}})

# Organize the partsupp data by part key and calculate total value
part_values = {}
for record in partsupp_data:
    part_key = record['PS_PARTKEY']
    value = record['PS_AVAILQTY'] * record['PS_SUPPLYCOST']
    if part_key in part_values:
        part_values[part_key] += value
    else:
        part_values[part_key] = value

# Get the total value of all parts
total_value = sum(part_values.values())

# Find parts that represent a significant percentage of the total value
important_parts = [{'part_number': part, 'value': value}
                   for part, value in part_values.items()
                   if value / total_value > 0.0001]

# Sort the parts by value in descending order
important_parts.sort(key=lambda x: x['value'], reverse=True)

# Write the results to 'query_output.csv'
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['part_number', 'value']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for part in important_parts:
        writer.writerow(part)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
