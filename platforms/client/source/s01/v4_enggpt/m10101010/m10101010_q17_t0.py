import pymysql
import pymongo
import csv

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
part_collection = mongo_db['part']

# Get qualifying part keys from MongoDB
qualifying_parts = part_collection.find({'P_BRAND': 'Brand#23', 'P_CONTAINER': 'MED BAG'}, {'P_PARTKEY': 1})
part_keys = [part['P_PARTKEY'] for part in qualifying_parts]

# If no qualifying parts, exit the script
if not part_keys:
    print('No qualifying parts found.')
    exit()

# Convert part keys to a format suitable for the IN clause in SQL
part_keys_str = ','.join(map(str, part_keys))

# MySQL query
mysql_query = """
SELECT L_PARTKEY, SUM(L_EXTENDEDPRICE) / 7.0 AS avg_yearly_extended_price
FROM lineitem
WHERE L_PARTKEY IN (%s) AND L_QUANTITY < (SELECT 0.2 * AVG(L_QUANTITY) FROM lineitem WHERE L_PARTKEY = lineitem.L_PARTKEY)
GROUP BY L_PARTKEY;
""" % part_keys_str
mysql_cursor.execute(mysql_query)
results = mysql_cursor.fetchall()

# Write to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    result_writer = csv.writer(csvfile)
    result_writer.writerow(['L_PARTKEY', 'avg_yearly_extended_price'])  # Header
    result_writer.writerows(results)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
