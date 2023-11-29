import pymysql
import pymongo
import csv
from datetime import datetime

# MySQL connection setup
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB connection setup
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Fetch nation key for CANADA from MySQL
mysql_cursor.execute("SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'CANADA'")
nation_keys = [row[0] for row in mysql_cursor.fetchall()]

# Fetch part keys for parts like the forest part from MySQL
mysql_cursor.execute("SELECT P_PARTKEY FROM part WHERE P_NAME LIKE '%forest%'")
part_keys = [row[0] for row in mysql_cursor.fetchall()]

# Fetch the supplier keys, parts they supplied, and aggregate quantities shipped within the date from MongoDB
supplier_info = mongo_db['lineitem'].aggregate([
    {
        '$match': {
            'L_SHIPDATE': {
                '$gte': datetime(1994, 1, 1),
                '$lt': datetime(1995, 1, 1)
            },
            'L_PARTKEY': {'$in': part_keys},
            'L_SUPPKEY': {'$exists': True}
        }
    },
    {
        '$group': {
            '_id': {'L_SUPPKEY': '$L_SUPPKEY', 'L_PARTKEY': '$L_PARTKEY'},
            'total_quantity': {'$sum': '$L_QUANTITY'}
        }
    }
])

supplier_thresholds = {}
for info in supplier_info:
  suppkey, partkey = info['_id']['L_SUPPKEY'], info['_id']['L_PARTKEY']
  if suppkey in supplier_thresholds:
    supplier_thresholds[suppkey][partkey] = info['total_quantity']
  else:
    supplier_thresholds[suppkey] = {partkey: info['total_quantity']}

# Fetch supplier details from MongoDB
suppliers = mongo_db['supplier'].find({'S_NATIONKEY': {'$in': nation_keys}})

# Determine the suppliers with an excess of parts
excess_suppliers = {}
for supplier in suppliers:
  s_suppkey = supplier['S_SUPPKEY']
  if s_suppkey in supplier_thresholds:
    excess_parts_count = sum(1 for qty in supplier_thresholds[s_suppkey].values() if qty > 50)
    if excess_parts_count > (len(part_keys) / 2):
      excess_suppliers[s_suppkey] = supplier['S_NAME']

# Write query results to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['S_SUPPKEY', 'S_NAME'])
    for suppkey, name in excess_suppliers.items():
        writer.writerow([suppkey, name])

# Close the connections
mysql_conn.close()
mongo_client.close()
