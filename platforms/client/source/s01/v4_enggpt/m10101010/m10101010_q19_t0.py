# query.py
import pymysql
import pymongo
import csv

# --- MySQL Connection ---
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# --- MongoDB Connection ---
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
part_collection = mongo_db["part"]

# Retrieve parts based on conditions
parts_query = {
    "$or": [
        {"P_BRAND": "Brand#12", "P_CONTAINER": {"$in": ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]}},
        {"P_BRAND": "Brand#23", "P_CONTAINER": {"$in": ["MED BAG", "MED BOX", "MED PKG", "MED PACK"]}},
        {"P_BRAND": "Brand#34", "P_CONTAINER": {"$in": ["LG CASE", "LG BOX", "LG PACK", "LG PKG"]}},
    ]
}
parts_projection = {"P_PARTKEY": 1}
selected_parts = list(part_collection.find(parts_query, parts_projection))
partkeys = [part["P_PARTKEY"] for part in selected_parts]

# Generate the MySQL query for lineitems
brand_conditions = [
    "(L_SHIPMODE IN ('AIR', 'AIR REG') AND L_SHIPINSTRUCT = 'DELIVER IN PERSON' \
    AND L_QUANTITY BETWEEN 1 AND 11 AND L_PARTKEY IN %(partkeys)s)",
    "(L_SHIPMODE IN ('AIR', 'AIR REG') AND L_SHIPINSTRUCT = 'DELIVER IN PERSON' \
    AND L_QUANTITY BETWEEN 10 AND 20 AND L_PARTKEY IN %(partkeys)s)",
    "(L_SHIPMODE IN ('AIR', 'AIR REG') AND L_SHIPINSTRUCT = 'DELIVER IN PERSON' \
    AND L_QUANTITY BETWEEN 20 AND 30 AND L_PARTKEY IN %(partkeys)s)"
]
mysql_query = f"""
SELECT
    L_PARTKEY,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS revenue
FROM
    lineitem
WHERE
    {" OR ".join(brand_conditions)}
GROUP BY
    L_PARTKEY;
"""

# Execute the MySQL query
mysql_cursor.execute(mysql_query, {'partkeys': partkeys})
lineitems = mysql_cursor.fetchall()

# Write output to CSV file
with open('query_output.csv', mode='w') as file:
    writer = csv.writer(file)
    # Writing the headers
    writer.writerow(['L_PARTKEY', 'REVENUE'])
    # Writing data rows
    for item in lineitems:
        writer.writerow(item)

# Close the connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
