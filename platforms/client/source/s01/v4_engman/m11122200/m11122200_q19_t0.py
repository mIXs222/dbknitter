import pymysql
import pymongo
import csv

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

# Define the criteria for the types as tuples containing the brand, containers, and size range
types_criteria = [
    ('Brand#12', ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'], (1, 5)),
    ('Brand#23', ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'], (1, 10)),
    ('Brand#34', ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'], (1, 15))
]

# Query MongoDB to get a list of all Partkeys that match the criteria.
matching_partkeys = []
for brand, containers, sizes in types_criteria:
    mongo_query = {
        "P_BRAND": brand,
        "P_CONTAINER": {"$in": containers},
        "P_SIZE": {"$gte": sizes[0], "$lte": sizes[1]}
    }
    partkeys = mongo_db.part.find(mongo_query, {"P_PARTKEY": 1})
    partkeys = [p["P_PARTKEY"] for p in partkeys]
    matching_partkeys.extend(partkeys)

# MySQL query to find the revenue
mysql_cursor = mysql_conn.cursor()
mysql_query = """
SELECT SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE
FROM lineitem
WHERE
    L_PARTKEY IN (%s) AND
    L_SHIPMODE IN ('AIR', 'AIR REG') AND
    L_SHIPINSTRUCT = 'DELIVER IN PERSON'
"""
in_clause = ', '.join(map(str, matching_partkeys))
mysql_cursor.execute(mysql_query % in_clause)
revenue = mysql_cursor.fetchone()[0]

# Write the output to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['REVENUE'])
    writer.writerow([revenue])

mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
