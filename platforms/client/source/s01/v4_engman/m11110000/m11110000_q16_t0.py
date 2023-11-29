import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    cursorclass=pymysql.cursors.Cursor
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Define the sizes the customer is interested in
sizes = [49, 14, 23, 45, 19, 3, 36, 9]

# MySQL Query
mysql_query = """
SELECT PS_PARTKEY
FROM partsupp
WHERE PS_PARTKEY NOT IN (
    SELECT P_PARTKEY
    FROM part
    WHERE P_SIZE IN (%s)
    AND P_TYPE NOT LIKE '%%MEDIUM POLISHED%%'
    AND P_BRAND <> 'Brand#45'
)
"""

# Execute MySQL Query
cursor = mysql_conn.cursor()
format_strings = ','.join(['%s'] * len(sizes))
cursor.execute(mysql_query % format_strings, tuple(sizes))
partkeys_with_suppliers = cursor.fetchall()
mysql_conn.close()

# Filter parts and suppliers from MongoDB
partkeys = [p[0] for p in partkeys_with_suppliers]
parts = mongo_db.part.find({
    'P_PARTKEY': {'$in': partkeys},
})
supplierkeys = [p['P_PARTKEY'] for p in parts]
suppliers = mongo_db.supplier.find({
    'S_SUPPKEY': {'$in': supplierkeys},
    'S_COMMENT': {'$not': {'$regex': '.*complaints.*'}}
})

# Combine and count the number of suppliers
final_data = {}
for supplier in suppliers:
    s_suppkey = supplier['S_SUPPKEY']
    s_name = supplier['S_NAME']
    key = (s_name, s_suppkey)
    final_data[key] = final_data.get(key, 0) + 1

# Sort by descending count and ascending brand, type and size
sorted_data = sorted(final_data.items(), key=lambda x: (-x[1], x[0]))

# Write to CSV file
with open('query_output.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['Sup_Name', 'Sup_Key', 'Count'])
    for data, count in sorted_data:
        writer.writerow([data[0], data[1], count])
