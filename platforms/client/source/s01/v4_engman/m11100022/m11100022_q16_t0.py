# query.py
import pymysql
import pymongo
import csv

# MySQL connection setup
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB connection setup
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
mongodb_part_collection = mongodb_db['part']

# Fetch parts that satisfy the customer requirements from MongoDB
sizes = [49, 14, 23, 45, 19, 3, 36, 9]
required_parts = mongodb_part_collection.find({
    "P_SIZE": {"$in": sizes},
    "P_TYPE": {"$ne": "MEDIUM POLISHED"},
    "P_BRAND": {"$ne": "Brand#45"}
}, {"P_PARTKEY": 1, "_id": 0})

part_keys = [p["P_PARTKEY"] for p in required_parts]

# Query corresponding suppliers from MySQL
supplier_query = """
SELECT PS_SUPPKEY, COUNT(PS_SUPPKEY) as supplier_count
FROM partsupp
WHERE PS_PARTKEY IN (%s)
GROUP BY PS_SUPPKEY
HAVING COUNT(PS_SUPPKEY) > 0
ORDER BY supplier_count DESC, PS_SUPPKEY ASC;
""" % ','.join(['%s'] * len(part_keys))

mysql_cursor.execute(supplier_query, part_keys)
suppliers = mysql_cursor.fetchall()

# Prepare the output file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['PS_SUPPKEY', 'supplier_count'])
    for row in suppliers:
        writer.writerow(row)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()
