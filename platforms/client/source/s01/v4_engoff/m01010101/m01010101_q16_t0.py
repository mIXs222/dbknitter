import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Get supplier keys with complaints from MongoDB
complaint_suppliers = set()
for supplier in mongo_db.supplier.find({"S_COMMENT": {"$regex": ".*Customer.*Complaints.*"}}):
    complaint_suppliers.add(supplier['S_SUPPKEY'])

# Define sizes and execute the query for MySQL
sizes = (49, 14, 23, 45, 19, 3, 36, 9)
try:
    with mysql_conn.cursor() as cursor:
        sql = """
        SELECT P_BRAND, P_TYPE, P_SIZE, COUNT(DISTINCT PS_SUPPKEY) as supplier_count
        FROM part JOIN partsupp ON part.P_PARTKEY = partsupp.PS_PARTKEY
        WHERE P_SIZE IN %s AND P_BRAND <> 'Brand#45' AND P_TYPE NOT LIKE 'MEDIUM POLISHED%%'
        GROUP BY P_BRAND, P_TYPE, P_SIZE 
        HAVING COUNT(DISTINCT PS_SUPPKEY) > 0
        ORDER BY supplier_count DESC, P_BRAND ASC, P_TYPE ASC, P_SIZE ASC;
        """
        cursor.execute(sql, (sizes,))
        results = cursor.fetchall()
finally:
    mysql_conn.close()

# Exclude suppliers with complaints
filtered_results = [
    row for row in results if row[3] not in complaint_suppliers
]

# Write results to CSV
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["P_BRAND", "P_TYPE", "P_SIZE", "SUPPLIER_COUNT"])
    for result in filtered_results:
        writer.writerow(result)
