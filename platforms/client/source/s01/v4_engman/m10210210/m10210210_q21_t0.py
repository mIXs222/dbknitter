import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
nation_collection = mongo_db['nation']
supplier_collection = mongo_db['supplier']

# Find N_NATIONKEY for 'SAUDI ARABIA'
saudi_arabia_nationkey = nation_collection.find_one({'N_NAME': 'SAUDI ARABIA'})['N_NATIONKEY']

# Find suppliers from 'SAUDI ARABIA'
saudi_suppliers = supplier_collection.find({'S_NATIONKEY': saudi_arabia_nationkey})
saudi_supplier_keys = [supplier['S_SUPPKEY'] for supplier in saudi_suppliers]

# Query to find orders with status 'F' that have multiple suppliers with only one failed to meet commit date
mysql_cursor.execute("""
SELECT L_SUPPKEY, COUNT(*) AS NUMWAIT
FROM lineitem
WHERE L_SUPPKEY IN %s
AND L_RETURNFLAG = 'F'
AND L_ORDERKEY IN (
    SELECT L_ORDERKEY
    FROM lineitem
    GROUP BY L_ORDERKEY
    HAVING COUNT(DISTINCT L_SUPPKEY) > 1
    AND MAX(L_COMMITDATE < L_RECEIPTDATE)
    AND SUM(L_COMMITDATE < L_RECEIPTDATE) = 1
)
GROUP BY L_SUPPKEY
ORDER BY NUMWAIT DESC, L_SUPPKEY;
""", [saudi_supplier_keys])

# Collect supplier names and number of await lineitems
results = []
for row in mysql_cursor.fetchall():
    supp_key, num_wait = row
    supplier_name = supplier_collection.find_one({'S_SUPPKEY': supp_key})['S_NAME']
    results.append((num_wait, supplier_name))

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(['NUMWAIT', 'S_NAME'])
    for num_wait, supplier_name in sorted(results, key=lambda x: (-x[0], x[1])):
        csv_writer.writerow([num_wait, supplier_name])
