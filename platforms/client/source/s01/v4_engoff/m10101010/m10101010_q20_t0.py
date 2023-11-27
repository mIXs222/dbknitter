import pymysql
import pymongo
import csv
from datetime import datetime

# Establish MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Establish MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# MySQL query to get the relevant lineitems and suppliers
mysql_query = """
    SELECT s.S_SUPPKEY, s.S_NAME, l.L_PARTKEY, SUM(l.L_QUANTITY) as total_quantity
    FROM supplier s
    JOIN lineitem l ON s.S_SUPPKEY = l.L_SUPPKEY
    WHERE l.L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01'
    GROUP BY s.S_SUPPKEY, l.L_PARTKEY
    HAVING total_quantity > 50
"""

# Execute MySQL query
mysql_cursor.execute(mysql_query)
mysql_result = mysql_cursor.fetchall()

# MongoDB query to get the relevant parts and part suppliers
mongo_parts = mongodb.part.find({"P_NAME": {"$regex": "forest"}}, {"P_PARTKEY": 1, "_id": 0})
part_keys = [part['P_PARTKEY'] for part in mongo_parts]

# Get the mapping of nation keys to names
nation_map = {nation['N_NATIONKEY']: nation['N_NAME'] for nation in mongodb.nation.find()}

# Process results and write to CSV
with open('query_output.csv', mode='w', newline='') as file:
    csv_writer = csv.writer(file)
    csv_writer.writerow(["SUPPLIER_NO", "SUPPLIER_NAME", "PART_NO", "TOTAL_QUANTITY"])

    for row in mysql_result:
        suppkey, s_name, partkey, total_quantity = row
        if partkey in part_keys:
            # Check if the supplier is from CANADA
            partsupp = mongodb.partsupp.find_one({"PS_PARTKEY": partkey, "PS_SUPPKEY": suppkey})
            if partsupp and nation_map.get(partsupp['PS_SUPPKEY']) == "CANADA":
                csv_writer.writerow([suppkey, s_name, partkey, total_quantity])

# Close the connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
