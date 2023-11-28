# query.py

import pymysql
import pymongo
import csv

# MySQL Connection
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')
cursor = connection.cursor()

# MongoDB Connection
client = pymongo.MongoClient('mongodb', 27017)
mongo_db = client['tpch']
part_col = mongo_db['part']

# Fetch data from MySQL
mysql_query = """
    SELECT PS_SUPPKEY, PS_PARTKEY
    FROM partsupp
    WHERE PS_SUPPKEY NOT IN (
        SELECT S_SUPPKEY FROM supplier WHERE S_COMMENT LIKE '%Customer Complaints%'
    )
"""
cursor.execute(mysql_query)
partsupp_data = cursor.fetchall()

# Convert MySQL data to dictionary for easy searching
partsupp_dict = {(ps_suppkey, ps_partkey) for ps_suppkey, ps_partkey in partsupp_data}

# Fetch data from MongoDB
mongo_query = {
    "P_PARTKEY": {"$in": [ps_partkey for _, ps_partkey in partsupp_dict]},
    "P_BRAND": {"$ne": "Brand#45"},
    "P_TYPE": {"$not": {"$regex": "^MEDIUM POLISHED.*"}},
    "P_SIZE": {"$in": [49, 14, 23, 45, 19, 3, 36, 9]}
}
part_data = list(part_col.find(mongo_query, {"P_PARTKEY": 1, "P_BRAND": 1, 
                                              "P_TYPE": 1, "P_SIZE": 1}))

# Aggregation
results = {}
for part in part_data:
    key = (part['P_BRAND'], part['P_TYPE'], part['P_SIZE'])
    results[key] = results.get(key, 0) + 1

# Sort the results
sorted_results = sorted(results.items(), key=lambda x: (-x[1], x[0]))

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['P_BRAND', 'P_TYPE', 'P_SIZE', 'SUPPLIER_CNT'])
    for (brand, p_type, size), count in sorted_results:
        csvwriter.writerow([brand, p_type, size, count])

# Close connections
cursor.close()
connection.close()
client.close()
