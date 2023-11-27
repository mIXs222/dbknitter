import pymongo
import pymysql
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    database="tpch"
)

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client['tpch']
part_collection = mongo_db['part']

# Setting up the containers for each type
type1_containers = {'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'}
type2_containers = {'MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'}
type3_containers = {'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'}

# Fetch type1 parts from MongoDB
type1_parts = list(part_collection.find({
    "P_BRAND": "Brand#12",
    "P_CONTAINER": {"$in": list(type1_containers)},
    "P_SIZE": {"$gte": 1, "$lte": 5}
}, {"_id": 0, "P_PARTKEY": 1}))

# Fetch type2 parts from MongoDB
type2_parts = list(part_collection.find({
    "P_BRAND": "Brand#23",
    "P_CONTAINER": {"$in": list(type2_containers)},
    "P_SIZE": {"$gte": 1, "$lte": 10}
}, {"_id": 0, "P_PARTKEY": 1}))

# Fetch type3 parts from MongoDB
type3_parts = list(part_collection.find({
    "P_BRAND": "Brand#34",
    "P_CONTAINER": {"$in": list(type3_containers)},
    "P_SIZE": {"$gte": 1, "$lte": 15}
}, {"_id": 0, "P_PARTKEY": 1}))

# Convert part documents to sets of part keys
type1_partkeys = {part['P_PARTKEY'] for part in type1_parts}
type2_partkeys = {part['P_PARTKEY'] for part in type2_parts}
type3_partkeys = {part['P_PARTKEY'] for part in type3_parts}

# Create a cursor object using the cursor() method
cursor = mysql_conn.cursor()

# Execute the query for MySQL
sql = """
SELECT L_PARTKEY, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS revenue
FROM lineitem
WHERE
    L_SHIPMODE IN ('AIR', 'AIR REG') AND
    L_SHIPINSTRUCT = 'DELIVER IN PERSON' AND
    ((L_PARTKEY IN %s AND L_QUANTITY BETWEEN 1 AND 11) OR
     (L_PARTKEY IN %s AND L_QUANTITY BETWEEN 10 AND 20) OR
     (L_PARTKEY IN %s AND L_QUANTITY BETWEEN 20 AND 30))
GROUP BY L_PARTKEY
"""
cursor.execute(sql, [tuple(type1_partkeys), tuple(type2_partkeys), tuple(type3_partkeys)])
result = cursor.fetchall()

# Writing to CSV
with open('query_output.csv', mode='w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['PARTKEY', 'REVENUE'])
    for row in result:
        writer.writerow(row)

# Closing the connections
cursor.close()
mysql_conn.close()
mongo_client.close()
