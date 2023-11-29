import csv
import pymysql
import pymongo

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Define the conditions for the types
conditions = [
    {"P_BRAND": "Brand#12", "P_CONTAINER": {"$in": ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]}, "P_SIZE": {"$gte": 1, "$lte": 5}},
    {"P_BRAND": "Brand#23", "P_CONTAINER": {"$in": ["MED BAG", "MED BOX", "MED PKG", "MED PACK"]}, "P_SIZE": {"$gte": 1, "$lte": 10}},
    {"P_BRAND": "Brand#34", "P_CONTAINER": {"$in": ["LG CASE", "LG BOX", "LG PACK", "LG PKG"]}, "P_SIZE": {"$gte": 1, "$lte": 15}}
]

# Find the part keys for the defined conditions
part_keys = set()
for condition in conditions:
    for part in mongodb.part.find(condition, {"_id": 0, "P_PARTKEY": 1}):
        part_keys.add(part["P_PARTKEY"])

# Format the keys for the SQL WHERE clause
formatted_keys = ','.join(str(pk) for pk in part_keys)

# SQL query using the part keys
sql = f"""
SELECT SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE
FROM lineitem
WHERE L_PARTKEY IN ({formatted_keys})
AND L_SHIPMODE IN ('AIR', 'AIR REG')
AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
"""

with mysql_conn.cursor() as cursor:
    cursor.execute(sql)
    result = cursor.fetchone()

# Write the result to the file
with open('query_output.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['REVENUE'])
    writer.writerow(result)

# Close database connections
mysql_conn.close()
mongo_client.close()
