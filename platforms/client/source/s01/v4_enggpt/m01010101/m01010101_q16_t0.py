import pymysql
import pymongo
import csv

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# MongoDB connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Prepare SQL query for MySQL data extraction
mysql_query = """
SELECT p.P_BRAND, p.P_TYPE, p.P_SIZE, PS_SUPPKEY
FROM part p
JOIN partsupp ps ON p.P_PARTKEY = ps.PS_PARTKEY
WHERE p.P_BRAND <> 'Brand#45'
AND p.P_TYPE NOT LIKE 'MEDIUM POLISHED%'
AND p.P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
"""

# Execute MySQL query and fetch data
cursor = mysql_conn.cursor()
cursor.execute(mysql_query)
mysql_parts = cursor.fetchall()

# Retrieve supplier keys with unwanted comments from MongoDB
unwanted_supplier_keys = set()
for s in mongodb_db.supplier.find({"S_COMMENT": {"$regex": ".*Customer Complaints.*"}}):
    unwanted_supplier_keys.add(s['S_SUPPKEY'])

# Combine results, exclude unwanted suppliers and count suppliers per group
grouped_data = {}
for row in mysql_parts:
    brand, type_, size, supp_key = row
    if supp_key not in unwanted_supplier_keys:
        group_key = (brand, type_, size)
        if group_key in grouped_data:
            grouped_data[group_key].add(supp_key)
        else:
            grouped_data[group_key] = {supp_key}

# Prepare data for CSV export
export_data = [(brand, type_, size, len(suppliers)) for (brand, type_, size), suppliers in grouped_data.items()]
export_data.sort(key=lambda x: (-x[3], x[0], x[1], x[2]))

# Write data to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['P_BRAND', 'P_TYPE', 'P_SIZE', 'SUPPLIER_CNT'])
    for row in export_data:
        writer.writerow(row)

# Close connections
cursor.close()
mysql_conn.close()
mongodb_client.close()
