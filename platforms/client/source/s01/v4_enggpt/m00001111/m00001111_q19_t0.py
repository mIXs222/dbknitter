# python code: query.py

import csv
import pymysql
import pymongo

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
lineitem_collection = mongo_db['lineitem']

# Fetch parts from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT * FROM part
        WHERE (P_BRAND = 'Brand#12' AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') 
            AND P_SIZE BETWEEN 1 AND 5)
        OR (P_BRAND = 'Brand#23' AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') 
            AND P_SIZE BETWEEN 1 AND 10)
        OR (P_BRAND = 'Brand#34' AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') 
            AND P_SIZE BETWEEN 1 AND 15)
    """)
    parts = {row[0]: row for row in cursor.fetchall()}

# Fetch lineitems from MongoDB
lineitems_cursor = lineitem_collection.find({
    '$or': [
        {'L_QUANTITY': {'$gte': 1, '$lte': 11}, 'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']}, 
         'L_SHIPINSTRUCT': 'DELIVER IN PERSON'},
        {'L_QUANTITY': {'$gte': 10, '$lte': 20}, 'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']}, 
         'L_SHIPINSTRUCT': 'DELIVER IN PERSON'},
        {'L_QUANTITY': {'$gte': 20, '$lte': 30}, 'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']}, 
         'L_SHIPINSTRUCT': 'DELIVER IN PERSON'}
    ]
})

# Calculate revenue and prepare record for CSV
revenue_data = []
for lineitem in lineitems_cursor:
    part = parts.get(lineitem['L_PARTKEY'])
    if part:
        revenue = lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])
        revenue_data.append([lineitem['L_ORDERKEY'], lineitem['L_PARTKEY'], revenue])

# Write to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['L_ORDERKEY', 'L_PARTKEY', 'REVENUE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    writer.writeheader()
    for row in revenue_data:
        writer.writerow({'L_ORDERKEY': row[0], 'L_PARTKEY': row[1], 'REVENUE': row[2]})

# Close connections
mysql_conn.close()
mongo_client.close()
