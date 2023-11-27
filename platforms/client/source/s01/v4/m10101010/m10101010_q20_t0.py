# query_database.py

import pymysql
import pymongo
import csv

# Connect to MySQL database
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB database
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']
parts_collection = mongo_db['part']
partsupp_collection = mongo_db['partsupp']
nation_collection = mongo_db['nation']

# Fetch all nations with the name "CANADA"
canada_nations = list(nation_collection.find(
    {"N_NAME": "CANADA"},
    {"N_NATIONKEY": 1}
))

nationkeys = [nation['N_NATIONKEY'] for nation in canada_nations]

# Find parts with P_NAME like 'forest%'
parts = list(parts_collection.find(
    {"P_NAME": {'$regex': '^forest'}},
    {"P_PARTKEY": 1}
))

partkeys = [part['P_PARTKEY'] for part in parts]

# Find suppliers in Canada with available quantity and aggregate partsupp
suppliers_in_canada = partsupp_collection.aggregate([
    {"$match": {
        "PS_PARTKEY": {"$in": partkeys},
        "PS_SUPPKEY": {"$exists": True}
    }},
    {"$group": {
        "_id": "$PS_SUPPKEY",
        "total_availability": {"$sum": "$PS_AVAILQTY"}
    }}
])

# Convert the supplier availability to a dict for easy lookup
supplier_avail_dict = {
    record['_id']: record['total_availability'] for record in suppliers_in_canada
}

# Find matching supplier keys
with mysql_connection.cursor() as cursor:
    query_output = []
    for supplier_key, total_avail in supplier_avail_dict.items():
        cursor.execute("""
            SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY
            FROM supplier
            WHERE S_SUPPKEY = %s
            AND S_NATIONKEY IN %s
        """, (supplier_key, tuple(nationkeys)))
        for supp in cursor.fetchall():
            suppkey = supp[0]
            name = supp[1]
            address = supp[2]
            cursor.execute("""
                SELECT 0.5 * SUM(L_QUANTITY) as half_sum
                FROM lineitem
                WHERE L_PARTKEY IN %s
                AND L_SUPPKEY = %s
                AND L_SHIPDATE >= '1994-01-01'
                AND L_SHIPDATE < '1995-01-01'
            """, (tuple(partkeys), suppkey))
            half_quantity_sum = cursor.fetchone()[0] or 0
            # Check if the supplier's availability is greater than half_quantity_sum
            if total_avail > half_quantity_sum:
                query_output.append((name, address))

# Write query output to file query_output.csv
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['S_NAME', 'S_ADDRESS'])  # Header
    for row in query_output:
        writer.writerow(row)

# Close connections
mysql_connection.close()
mongo_client.close()
