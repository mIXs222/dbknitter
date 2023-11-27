import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Fetch suppliers and nation data from MySQL
mysql_cursor.execute("SELECT S.S_SUPPKEY, S.S_NAME FROM supplier AS S INNER JOIN nation AS N ON S.S_NATIONKEY = N.N_NATIONKEY WHERE N.N_NAME = 'SAUDI ARABIA'")
suppliers_for_nation = {row[0]: row[1] for row in mysql_cursor.fetchall()}

# Fetch orders with status 'F' from MySQL
mysql_cursor.execute("SELECT O.O_ORDERKEY FROM orders AS O WHERE O.O_ORDERSTATUS = 'F'")
orders_with_status_F = {row[0] for row in mysql_cursor.fetchall()}

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
lineitem_collection = mongo_db['lineitem']

# Query lineitems that have a shipdate later than the commitdate
late_lineitems = lineitem_collection.find({"L_SHIPDATE": {"$gt": "$L_COMMITDATE"}})

# Find orders which are late and match the orders with status 'F'
late_order_keys = set()
for lineitem in late_lineitems:
    if lineitem['L_ORDERKEY'] in orders_with_status_F:
        late_order_keys.add(lineitem['L_ORDERKEY'])

# Find suppliers who are the only ones late in their orders
suppliers_to_output = {}
for order_key in late_order_keys:
    lineitems_in_order = lineitem_collection.find({"L_ORDERKEY": order_key})
    
    # Get suppliers for each late item
    suppliers_in_order = set()
    for lineitem in lineitems_in_order:
        suppliers_in_order.add(lineitem['L_SUPPKEY'])

    # If there's only one supplier and it's in suppliers_for_nation, add to output
    if len(suppliers_in_order) == 1:
        supp_key = list(suppliers_in_order)[0]
        if supp_key in suppliers_for_nation:
            suppliers_to_output[supp_key] = suppliers_for_nation[supp_key]

# Write to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['S_SUPPKEY', 'S_NAME'])
    for supp_key, supp_name in suppliers_to_output.items():
        csvwriter.writerow([supp_key, supp_name])

mongo_client.close()
