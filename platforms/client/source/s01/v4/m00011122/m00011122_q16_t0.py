# query_execute.py

import pymysql
import pymongo
import csv

# Connection to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connection to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
supplier_collection = mongo_db["supplier"]

# Execute query on mysql to get filtered part details
mysql_cursor = mysql_connection.cursor()
mysql_cursor.execute(
    "SELECT P_PARTKEY, P_BRAND, P_TYPE, P_SIZE "
    "FROM part "
    "WHERE P_BRAND <> 'Brand#45' "
    "AND P_TYPE NOT LIKE 'MEDIUM POLISHED%' "
    "AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)"
)
part_details = mysql_cursor.fetchall()

# Get the S_SUPPKEY for suppliers with comments not like '%Customer%Complaints%'
excluded_suppliers = supplier_collection.find(
    {"S_COMMENT": {"$not": {"$regex": "Customer.*Complaints"}}},
    {"S_SUPPKEY": 1}
)
excluded_suppkeys = [supplier['S_SUPPKEY'] for supplier in excluded_suppliers]

# MongoDB query for partsupp
partsupp_results = mongo_db["partsupp"].aggregate([
    {"$match": {
        "PS_PARTKEY": {"$in": [part[0] for part in part_details]},
        "PS_SUPPKEY": {"$nin": excluded_suppkeys}
    }},
    {"$group": {
        "_id": {
            "PS_PARTKEY": "$PS_PARTKEY",
            "PS_SUPPKEY": "$PS_SUPPKEY"
        }
    }},
    {"$group": {
        "_id": "$_id.PS_PARTKEY",
        "SUPPLIER_CNT": {"$sum": 1}
    }}
])
partsupp_suppliers = {doc['_id']: doc['SUPPLIER_CNT'] for doc in partsupp_results}

# Final result combining mysql and mongodb data
final_result = []
for part in part_details:
    p_partkey, p_brand, p_type, p_size = part
    supplier_cnt = partsupp_suppliers.get(p_partkey, 0)
    final_result.append((p_brand, p_type, p_size, supplier_cnt))

# Sort the results as required in the query
final_result.sort(key=lambda x: (-x[3], x[0], x[1], x[2]))

# Write to CSV
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['P_BRAND', 'P_TYPE', 'P_SIZE', 'SUPPLIER_CNT'])
    for row in final_result:
        writer.writerow(row)

# Close connections
mysql_cursor.close()
mysql_connection.close()
mongo_client.close()
