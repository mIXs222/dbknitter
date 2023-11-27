import pymongo
import pymysql
import csv
from datetime import datetime

# Establishing connections
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# MySQL query to select suppliers and parts
mysql_query = """
SELECT s.S_SUPPKEY, s.S_NAME, p.P_PARTKEY, p.P_NAME
FROM supplier AS s
JOIN part AS p ON s.S_SUPPKEY = p.P_PARTKEY
WHERE p.P_NAME LIKE '%forest%'
"""

# Execute the MySQL query
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute(mysql_query)

# Get suppliers and parts from mysql
suppliers_parts = mysql_cursor.fetchall()

# MongoDB query for partsupp (for excess) and lineitem (for period constraint)
start_date = datetime(1994, 1, 1)
end_date = datetime(1995, 1, 1)

# Perform the MongoDB part of the query
parts_excess = {}
for doc in mongodb_db['partsupp'].find():
    if doc['PS_PARTKEY'] in [part[2] for part in suppliers_parts]:
        parts_excess.setdefault(doc['PS_SUPPKEY'], []).append(doc)

suppliers_shipped = {}
for doc in mongodb_db['lineitem'].find(
        {
            'L_SHIPDATE': {'$gte': start_date, '$lt': end_date},
            'L_PARTKEY': {'$in': [part[2] for part in suppliers_parts]}
        }
    ):
    suppliers_shipped[doc['L_SUPPKEY']] = suppliers_shipped.get(doc['L_SUPPKEY'], 0) + doc['L_QUANTITY']

# Finding suppliers with excess of forest parts
suppliers_with_excess = []
for supplier in suppliers_parts:
    suppkey, name, partkey, pname = supplier
    total_supplied = suppliers_shipped.get(suppkey, 0)
    total_excess = sum([item['PS_AVAILQTY'] for item in parts_excess.get(suppkey, [])])
    
    # Check excess condition
    if total_excess > total_supplied / 2:
        suppliers_with_excess.append([suppkey, name, total_excess])

# Closing connections
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()

# Write output to CSV
output_fields = ['S_SUPPKEY', 'S_NAME', 'TOTAL_EXCESS']
with open('query_output.csv', 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(output_fields)
    for row in suppliers_with_excess:
        csv_writer.writerow(row)
