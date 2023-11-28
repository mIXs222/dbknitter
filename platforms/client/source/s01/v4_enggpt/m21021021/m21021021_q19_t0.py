import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

mysql_query = """
SELECT P_PARTKEY, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE
FROM part
WHERE (P_BRAND = 'Brand#12' AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND P_SIZE BETWEEN 1 AND 5) OR
      (P_BRAND = 'Brand#23' AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND P_SIZE BETWEEN 1 AND 10) OR
      (P_BRAND = 'Brand#34' AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND P_SIZE BETWEEN 1 AND 15)
"""

mysql_cursor.execute(mysql_query)
part_results = {row[0]: row[1:] for row in mysql_cursor.fetchall()}

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
lineitem_collection = mongodb_db['lineitem']

lineitem_query = {
    "$or": [
        {"L_QUANTITY": {"$between": [1, 11]}, "L_SHIPMODE": {"$in": ["AIR", "AIR REG"]}, "L_SHIPINSTRUCT": "DELIVER IN PERSON"},
        {"L_QUANTITY": {"$between": [10, 20]}, "L_SHIPMODE": {"$in": ["AIR", "AIR REG"]}, "L_SHIPINSTRUCT": "DELIVER IN PERSON"},
        {"L_QUANTITY": {"$between": [20, 30]}, "L_SHIPMODE": {"$in": ["AIR", "AIR REG"]}, "L_SHIPINSTRUCT": "DELIVER IN PERSON"}
    ]
}

revenue = 0
lineitems = lineitem_collection.find(lineitem_query)
for lineitem in lineitems:
    part_data = part_results.get(lineitem['L_PARTKEY'])
    if part_data:
        extended_price = lineitem['L_EXTENDEDPRICE']
        discount = lineitem['L_DISCOUNT']
        revenue += extended_price * (1 - discount)

# Write the result to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['Total_Revenue'])
    csvwriter.writerow([revenue])

# Clean up resources
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()
