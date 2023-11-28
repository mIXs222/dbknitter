import csv
import pymysql
from pymongo import MongoClient
from datetime import datetime

# Establish connection to MySQL
mysql_conn = pymysql.connect(
    host='mysql', 
    user='root', 
    password='my-secret-pw', 
    db='tpch'
)
# MongoDB Connection
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Prepare sql and no-sql queries
mysql_query = """
SELECT L1.L_SUPPKEY, COUNT(L1.L_ORDERKEY) AS NUMWAIT
FROM lineitem L1
JOIN orders O ON L1.L_ORDERKEY = O.O_ORDERKEY
WHERE O.O_ORDERSTATUS = 'F'
AND L1.L_RECEIPTDATE > L1.L_COMMITDATE
AND EXISTS (
    SELECT L2.L_ORDERKEY FROM lineitem L2
    WHERE L2.L_ORDERKEY = L1.L_ORDERKEY AND L2.L_SUPPKEY <> L1.L_SUPPKEY
)
AND NOT EXISTS (
    SELECT L3.L_ORDERKEY FROM lineitem L3
    WHERE L3.L_ORDERKEY = L1.L_ORDERKEY
    AND L3.L_SUPPKEY <> L1.L_SUPPKEY
    AND L3.L_RECEIPTDATE > L1.L_COMMITDATE
)
GROUP BY L1.L_SUPPKEY
"""

# Execute MySQL Query
cursor = mysql_conn.cursor()
cursor.execute(mysql_query)
mysql_suppliers_data = cursor.fetchall()

# Retrieve MongoDB data
nation_data = mongo_db.nation.find({"N_NAME": "SAUDI ARABIA"}, {"N_NATIONKEY": 1})
nation_keys = [n['N_NATIONKEY'] for n in nation_data]

supplier_data = mongo_db.supplier.find(
    {"S_NATIONKEY": {"$in": nation_keys}},
    {"S_SUPPKEY": 1, "S_NAME": 1}
)
suppliers_info = {s['S_SUPPKEY']: s['S_NAME'] for s in supplier_data}

# Combine the results
combined_data = []
for supp_key, numwait in mysql_suppliers_data:
    if supp_key in suppliers_info:
        combined_data.append((suppliers_info[supp_key], numwait))

# Sort the results as per requirements
combined_data.sort(key=lambda x: (-x[1], x[0]))

# Write the results to query_output.csv
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['S_NAME', 'NUMWAIT'])
    for row in combined_data:
        writer.writerow(row)

# Close connections
cursor.close()
mysql_conn.close()
mongo_client.close()
