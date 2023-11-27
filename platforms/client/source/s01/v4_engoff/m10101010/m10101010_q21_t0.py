import pymysql
import pymongo
import csv

# Connect to MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB database
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Get nationkey for Saudi Arabia from MongoDB
nation_key = None
for nation in mongodb.nation.find({'N_NAME': 'SAUDI ARABIA'}):
    nation_key = nation['N_NATIONKEY']
    
# Get supplier details from MySQL where nationkey matches
suppliers = {}
cursor = mysql_conn.cursor()
cursor.execute("""
SELECT s.S_SUPPKEY, s.S_NAME, s.S_ADDRESS, s.S_PHONE, s.S_ACCTBAL, s.S_COMMENT 
FROM supplier s 
WHERE s.S_NATIONKEY = %s
""", (nation_key,))
for row in cursor:
    suppliers[row[0]] = row

# Get orders with status 'F' from MongoDB
order_keys = set()
for order in mongodb.orders.find({'O_ORDERSTATUS': 'F'}):
    order_keys.add(order['O_ORDERKEY'])

# Get unmatched line items from MySQL
unmatched_suppliers = {}
cursor.execute("""
SELECT l.L_ORDERKEY, l.L_SUPPKEY, MAX(l.L_COMMITDATE) as max_commit, MAX(l.L_RECEIPTDATE) as max_receipt 
FROM lineitem l 
WHERE l.L_ORDERKEY IN %s
GROUP BY l.L_ORDERKEY, l.L_SUPPKEY
HAVING max_commit < max_receipt
""", (tuple(order_keys),))

for row in cursor:
    order_key, supp_key, max_commit, max_receipt = row
    if supp_key in suppliers and order_key in order_keys:
        unmatched_suppliers[supp_key] = suppliers[supp_key]

# Write the results to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])

    for supp_key, supplier in unmatched_suppliers.items():
        writer.writerow(supplier)

# Close the database connections
cursor.close()
mysql_conn.close()
mongo_client.close()
