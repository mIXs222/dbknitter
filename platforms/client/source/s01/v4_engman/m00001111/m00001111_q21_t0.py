import pymysql
import pymongo
import csv

# Open a connection to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.Cursor
)

# Open a connection to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongodb = mongo_client["tpch"]

# MySQL query to retrieve suppliers of the given nation
mysql_query = """
SELECT S_SUPPKEY, S_NAME 
FROM supplier 
JOIN nation ON supplier.S_NATIONKEY = nation.N_NATIONKEY 
WHERE nation.N_NAME='SAUDI ARABIA';
"""

# Dictionary to hold supplier information from MySQL
suppliers = {}
with mysql_connection.cursor() as cursor:
    cursor.execute(mysql_query)
    for row in cursor:
        suppliers[row[0]] = row[1]

# MongoDB query to retrieve all the line items
orders_failed = mongodb['orders'].find({"O_ORDERSTATUS": "F"})
failed_order_ids = [order["O_ORDERKEY"] for order in orders_failed]

# Query to find line items for failed orders
line_items = list(mongodb['lineitem'].aggregate([
    {"$match": {"L_ORDERKEY": {"$in": failed_order_ids}}},
    {"$group": {"_id": {"L_ORDERKEY": "$L_ORDERKEY", "L_SUPPKEY": "$L_SUPPKEY"}, "count": {"$sum": 1}}},
    {"$match": {"count": {"$gt": 1}}}
]))

# Extract suppliers that kept the order waiting
waiting_suppliers = {}
for item in line_items:
    supplier_key = item['_id']['L_SUPPKEY']
    if supplier_key in suppliers:
        waiting_suppliers[supplier_key] = waiting_suppliers.get(supplier_key, 0) + item['count']

# Write the resulting data to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['NUMWAIT', 'S_NAME']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for supp_key, num_wait in sorted(waiting_suppliers.items(), key=lambda x: (-x[1], suppliers[x[0]])):
        writer.writerow({'NUMWAIT': num_wait, 'S_NAME': suppliers[supp_key]})

# Close the MySQL connection
mysql_connection.close()

# Close the MongoDB connection
mongo_client.close()
