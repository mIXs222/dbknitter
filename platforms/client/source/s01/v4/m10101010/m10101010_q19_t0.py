# query.py
import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
part_collection = mongo_db['part']

# Fetch part data from MongoDB based on conditions
part_query = {'$or': [
    {"P_BRAND": "Brand#12", "P_CONTAINER": {'$in': ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]}, "P_SIZE": {'$gte': 1, '$lte': 5}},
    {"P_BRAND": "Brand#23", "P_CONTAINER": {'$in': ["MED BAG", "MED BOX", "MED PKG", "MED PACK"]}, "P_SIZE": {'$gte': 1, '$lte': 10}},
    {"P_BRAND": "Brand#34", "P_CONTAINER": {'$in': ["LG CASE", "LG BOX", "LG PACK", "LG PKG"]}, "P_SIZE": {'$gte': 1, '$lte': 15}}
]}
parts = part_collection.find(part_query, {'P_PARTKEY': 1})
part_keys = [p['P_PARTKEY'] for p in parts]

# Construct the SQL query for lineitem data
lineitem_query = f"""
SELECT 
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE 
FROM 
    lineitem
WHERE 
    L_PARTKEY IN ({",".join(map(str, part_keys))})
    AND L_QUANTITY >= 1 AND L_QUANTITY <= 31
    AND L_SHIPMODE IN ('AIR', 'AIR REG')
    AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
"""

# Execute the lineitem query
mysql_cursor.execute(lineitem_query)

# Write the results to a CSV file
with open("query_output.csv", "w", newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(['REVENUE'])  # header
    for row in mysql_cursor:
        csv_writer.writerow(row)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()

print("Query output has been written to query_output.csv.")
