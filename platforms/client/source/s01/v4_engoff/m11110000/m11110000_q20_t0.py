# potential_part_promotion.py
import pymysql
import pymongo
import csv
from datetime import datetime

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
cursor = mysql_conn.cursor()

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Initial query for finding all parts that are forest related
forest_related_parts_query = """
SELECT L_PARTKEY, SUM(L_QUANTITY) as total_quantity
FROM lineitem
WHERE L_SHIPDATE >= '1994-01-01' AND L_SHIPDATE < '1995-01-01'
GROUP BY L_PARTKEY
HAVING SUM(L_QUANTITY) > 50;
"""
cursor.execute(forest_related_parts_query)
forest_related_parts = cursor.fetchall()

forest_related_partkeys = [row[0] for row in forest_related_parts]

# Retrieve parts with forest in their names from MongoDB
part_collection = mongo_db['part']
forest_parts = part_collection.find({'P_PARTKEY': {'$in': forest_related_partkeys}, 'P_NAME': {'$regex': 'forest', '$options': 'i'}})

# Create a list with P_PARTKEY from forest parts
forest_partkeys = [part['P_PARTKEY'] for part in forest_parts]

# Query for finding suppliers from Canada in MySQL
suppliers_in_canada_query = """
SELECT S_SUPPKEY
FROM supplier
WHERE S_NATIONKEY = (
    SELECT N_NATIONKEY
    FROM nation
    WHERE N_NAME = 'CANADA'
);
"""
cursor.execute(suppliers_in_canada_query)
suppliers_in_canada = cursor.fetchall()

supplier_keys = [row[0] for row in suppliers_in_canada]

# Filtering lineitems from the forest-specific parts
forest_part_suppliers_query = """
SELECT PS_SUPPKEY, PS_PARTKEY, SUM(PS_AVAILQTY) AS total_availqty
FROM partsupp
WHERE PS_PARTKEY IN ({}) AND PS_SUPPKEY IN ({})
GROUP BY PS_SUPPKEY, PS_PARTKEY;
""".format(','.join(map(str, forest_partkeys)), ','.join(map(str, supplier_keys)))
cursor.execute(forest_part_suppliers_query)
forest_part_suppliers = cursor.fetchall()

# Writing the results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['Supplier Key', 'Part Key', 'Total Available Quantity'])
    for row in forest_part_suppliers:
        csvwriter.writerow(row)

# Closing connections
cursor.close()
mysql_conn.close()
mongo_client.close()
