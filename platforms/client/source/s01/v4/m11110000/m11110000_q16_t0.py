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
mongodb_db = mongodb_client.tpch

# Fetch data from MySQL database
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT PS_PARTKEY, PS_SUPPKEY
        FROM partsupp
    """)
    partsupp_data = cursor.fetchall()

# Convert MySQL result to dictionary for easier processing
partsupp_dict = {}
for row in partsupp_data:
    P_PARTKEY, PS_SUPPKEY = row
    if P_PARTKEY not in partsupp_dict:
        partsupp_dict[P_PARTKEY] = []
    partsupp_dict[P_PARTKEY].append(PS_SUPPKEY)

# Fetch not allowed SUPPKEYs from MongoDB
not_allowed_suppkeys = set()
for row in mongodb_db.supplier.find({"S_COMMENT": {"$regex": ".*Customer.*Complaints.*"}}):
    not_allowed_suppkeys.add(row['S_SUPPKEY'])

# Fetch parts from MongoDB and process data
query_result = []
for part in mongodb_db.part.find({
    "P_BRAND": {"$ne": 'Brand#45'},
    "P_TYPE": {"$not": {"$regex": "^MEDIUM POLISHED"}},
    "P_SIZE": {"$in": [49, 14, 23, 45, 19, 3, 36, 9]}
}):
    P_PARTKEY = part['P_PARTKEY']
    if P_PARTKEY in partsupp_dict:
        supplier_cnt = 0
        for PS_SUPPKEY in partsupp_dict[P_PARTKEY]:
            if PS_SUPPKEY not in not_allowed_suppkeys:
                supplier_cnt += 1
        if supplier_cnt > 0:
            query_result.append(
                [part['P_BRAND'], part['P_TYPE'], part['P_SIZE'], supplier_cnt]
            )

# Sort and write results to CSV file
query_result.sort(key=lambda row: (-row[3], row[0], row[1], row[2]))
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['P_BRAND', 'P_TYPE', 'P_SIZE', 'SUPPLIER_CNT'])
    for row in query_result:
        writer.writerow(row)

# Close the connections
mysql_conn.close()
mongodb_client.close()
