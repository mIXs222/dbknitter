# query.py

import pymysql
import pymongo
import csv
from datetime import datetime

# Connection to MySQL
mysql_conn = pymysql.connect(
    host='mysql', 
    user='root', 
    password='my-secret-pw', 
    database='tpch'
)

# Connection to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Query for supplier and lineitem from MySQL
with mysql_conn.cursor() as cursor:
    # Find all supplier keys that are from the nation 'CANADA'
    cursor.execute("""
        SELECT supplier.S_SUPPKEY, supplier.S_NAME, supplier.S_ADDRESS
        FROM supplier
            INNER JOIN nation ON supplier.S_NATIONKEY = nation.N_NATIONKEY
        WHERE nation.N_NAME = 'CANADA'
    """)
    suppliers_from_canada = cursor.fetchall()

# Filter out suppliers with parts that have names starting with 'forest'
supplier_keys_with_forest_parts = set()
for partsupp in mongo_db['partsupp'].find():
    part = mongo_db['part'].find_one({'P_PARTKEY': partsupp['PS_PARTKEY']})
    if part and part['P_NAME'].startswith('forest'):
        supplier_keys_with_forest_parts.add(partsupp['PS_SUPPKEY'])

# Determine which suppliers meet the line item criteria using MySQL
qualified_suppliers = []
for supplier_key, supplier_name, supplier_address in suppliers_from_canada:
    if supplier_key in supplier_keys_with_forest_parts:
        with mysql_conn.cursor() as cursor:
            cursor.execute("""
            SELECT SUM(L_QUANTITY), PS_PARTKEY
            FROM lineitem
            WHERE 
                L_SUPPKEY = %s AND
                L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01'
            GROUP BY L_PARTKEY
            HAVING SUM(L_QUANTITY) > 
            (SELECT 0.5*SUM(L_QUANTITY)
             FROM lineitem
             WHERE 
                 L_SUPPKEY = %s AND
                 L_PARTKEY = PS_PARTKEY AND
                 L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01')
            """, (supplier_key, supplier_key))
            if cursor.rowcount > 0:
                qualified_suppliers.append((supplier_name, supplier_address))

qualified_suppliers.sort()

# Write the output to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['S_NAME', 'S_ADDRESS'])
    for supplier_name, supplier_address in qualified_suppliers:
        writer.writerow([supplier_name, supplier_address])

# Close the connections
mysql_conn.close()
mongo_client.close()
