import pymysql
import pymongo
import csv
from bson.decimal128 import Decimal128

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
part_collection = mongo_db['part']

# Fetch the MongoDB data and store it in memory
mongo_parts = list(part_collection.find({"P_BRAND": "Brand#23", "P_CONTAINER": "MED BAG"}, {"_id": 0, "P_PARTKEY": 1}))

# Convert MongoDB data to a structure suitable for SQL WHERE IN statement
partkeys = [str(part['P_PARTKEY']) for part in mongo_parts]

# Execute nested queries in MySQL
query = """
SELECT
    SUM(L_EXTENDEDPRICE) / 7.0 AS AVG_YEARLY
FROM
    lineitem
WHERE
    L_PARTKEY IN ({})
    AND L_QUANTITY < (
        SELECT 
            0.2 * AVG(L_QUANTITY)
        FROM 
            lineitem
        WHERE 
            L_PARTKEY IN({})
    )
""".format(','.join(partkeys), ','.join(partkeys))
mysql_cursor.execute(query)

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['AVG_YEARLY'])
    for row in mysql_cursor:
        avg_yearly = float(row[0]) if row[0] is not None else 'NULL'
        csvwriter.writerow([avg_yearly])

# Close all connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
