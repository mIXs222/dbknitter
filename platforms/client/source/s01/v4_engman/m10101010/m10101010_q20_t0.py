import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_tpch = mongodb_client['tpch']

# MySQL query to fetch required data
mysql_query = """
    SELECT s.S_SUPPKEY, s.S_NAME, l.L_PARTKEY, l.L_QUANTITY
      FROM supplier AS s
      JOIN lineitem AS l ON s.S_SUPPKEY = l.L_SUPPKEY
      WHERE l.L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01';
"""
mysql_cursor.execute(mysql_query)
mysql_supplier_part_data = mysql_cursor.fetchall()

# MongoDB queries to fetch required data
nation_col = mongodb_tpch['nation']
part_col = mongodb_tpch['part']
partsupp_col = mongodb_tpch['partsupp']

canada_nation = nation_col.find_one({'N_NAME': 'CANADA'})
canada_nationkey = canada_nation['N_NATIONKEY'] if canada_nation else None

forest_parts = part_col.find({'P_NAME': {'$regex': 'forest', '$options': 'i'}})
forest_part_keys = [part['P_PARTKEY'] for part in forest_parts]

# Aggregation in MongoDB to find excess supply in the CANADA region
pipeline = [
    {'$match': {'PS_SUPPKEY': {'$in': [s[0] for s in mysql_supplier_part_data]}}},
    {'$group': {'_id': '$PS_SUPPKEY', 'total_supply': {'$sum': '$PS_AVAILQTY'}}}
]
partsupp_result = list(partsupp_col.aggregate(pipeline))

excess_suppliers = {}
for record in partsupp_result:
    total_supply = record['total_supply']
    for elem in mysql_supplier_part_data:
        if elem[0] == record['_id'] and elem[2] in forest_part_keys:
            # Check if quantity is more than 50% of total parts like forest part
            if elem[3] > total_supply / 2:
                excess_suppliers[elem[0]] = elem[1]  # Supplier key to name

# Writing results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    query_output_writer = csv.writer(csvfile)
    query_output_writer.writerow(['S_SUPPKEY', 'S_NAME'])

    for suppkey, sname in excess_suppliers.items():
        query_output_writer.writerow([suppkey, sname])
        
# Close connections
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()
