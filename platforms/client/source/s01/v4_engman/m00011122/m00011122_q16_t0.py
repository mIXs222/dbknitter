import pymysql
import pymongo
import csv

# Connection to MySQL
mysql_conn = pymysql.connect(user='root', password='my-secret-pw', database='tpch', host='mysql')

# Connection to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Query MySQL
with mysql_conn.cursor() as cursor:
    query_mysql = """
    SELECT P_PARTKEY, P_BRAND, P_TYPE, P_SIZE
    FROM part
    WHERE P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
      AND P_TYPE NOT LIKE 'MEDIUM POLISHED'
      AND P_BRAND NOT LIKE 'Brand#45'
    """
    cursor.execute(query_mysql)
    mysql_parts = cursor.fetchall()

# Query MongoDB (supplier and partsupp)
suppliers = list(mongodb['supplier'].find({"S_COMMENT": {"$not": {"$regex": ".*complaints.*"}}}))
supplier_keys = [s['S_SUPPKEY'] for s in suppliers]
partsupps = mongodb['partsupp'].find({"PS_SUPPKEY": {"$in": supplier_keys}})

# Find matching suppliers for each part
result = {}
for partsupp in partsupps:
    for part in mysql_parts:
        if part[0] == partsupp['PS_PARTKEY']:
            key = (part[1], part[2], part[3])
            result[key] = result.get(key, 0) + 1

# Sort results
sorted_result = sorted(result.items(), key=lambda x: (-x[1], x[0][1], x[0][2], x[0][0]))

# Write result to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['P_BRAND', 'P_TYPE', 'P_SIZE', 'SUPPLIER_COUNT'])
    for key, count in sorted_result:
        writer.writerow([key[0], key[1], key[2], count])

# Close connections
mysql_conn.close()
mongo_client.close()
