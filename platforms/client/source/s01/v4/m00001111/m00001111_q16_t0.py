# query.py
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
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Fetch the distinct `S_SUPPKEY` from the MySQL `supplier` table with the specified condition
mysql_cursor = mysql_conn.cursor()
excluded_suppliers_sql = """
SELECT DISTINCT S_SUPPKEY
FROM supplier
WHERE S_COMMENT LIKE '%Customer%Complaints%'
"""
mysql_cursor.execute(excluded_suppliers_sql)

excluded_suppliers = [row[0] for row in mysql_cursor.fetchall()]

# Close the cursor for good practice
mysql_cursor.close()

# Build the MongoDB pipeline for aggregation
mongo_pipeline = [
    {'$match': {
        'PS_SUPPKEY': {'$nin': excluded_suppliers},
        'PS_PARTKEY': {'$exists': True}
    }},
    {'$lookup': {
        'from': 'part',
        'localField': 'PS_PARTKEY',
        'foreignField': 'P_PARTKEY',
        'as': 'part_info'
    }},
    {'$unwind': '$part_info'},
    {'$match': {
        'part_info.P_BRAND': {'$ne': 'Brand#45'},
        'part_info.P_TYPE': {'$not': {'$regex': '^MEDIUM POLISHED'}},
        'part_info.P_SIZE': {'$in': [49, 14, 23, 45, 19, 3, 36, 9]}
    }},
    {'$group': {
        '_id': {
            'P_BRAND': '$part_info.P_BRAND',
            'P_TYPE': '$part_info.P_TYPE',
            'P_SIZE': '$part_info.P_SIZE'
        },
        'SUPPLIER_CNT': {'$sum': 1}
    }},
    {'$sort': {
        'SUPPLIER_CNT': -1,
        '_id.P_BRAND': 1,
        '_id.P_TYPE': 1,
        '_id.P_SIZE': 1
    }}
]

# Perform the aggregation on MongoDB
mongo_results = list(mongodb['partsupp'].aggregate(mongo_pipeline))

# Write the query output to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['P_BRAND', 'P_TYPE', 'P_SIZE', 'SUPPLIER_CNT']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    writer.writeheader()
    for record in mongo_results:
        writer.writerow({
            'P_BRAND': record['_id']['P_BRAND'],
            'P_TYPE': record['_id']['P_TYPE'],
            'P_SIZE': record['_id']['P_SIZE'],
            'SUPPLIER_CNT': record['SUPPLIER_CNT'],
        })

# Close connections
mysql_conn.close()
mongo_client.close()
