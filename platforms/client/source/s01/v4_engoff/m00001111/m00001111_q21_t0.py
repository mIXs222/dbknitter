# suppliers_who_kept_orders_waiting.py

import pymysql
import pymongo
import csv

# Connect to MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB database
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# SQL query to retrieve suppliers from the nation 'SAUDI ARABIA'
sql_query = """
SELECT S_SUPPKEY, S_NAME, S_ADDRESS
FROM supplier
JOIN nation ON supplier.S_NATIONKEY = nation.N_NATIONKEY
WHERE N_NAME = 'SAUDI ARABIA';
"""

# Execute the SQL query
with mysql_conn.cursor() as cursor:
    cursor.execute(sql_query)
    suppliers = {row[0]: {'S_NAME': row[1], 'S_ADDRESS': row[2]} for row in cursor.fetchall()}

# MongoDB query to find orders with status 'F' and late lineitems
orders_cursor = mongo_db.orders.find({"O_ORDERSTATUS": "F"})
orderkeys_with_status_f = [order['O_ORDERKEY'] for order in orders_cursor]

lineitems_cursor = mongo_db.lineitem.find(
    {"$and": [{"L_SUPPKEY": {"$in": list(suppliers.keys())}}, {"L_ORDERKEY": {"$in": orderkeys_with_status_f}}]}
)
late_orders_by_supplier = {}
for lineitem in lineitems_cursor:
    if lineitem['L_RECEIPTDATE'] > lineitem['L_COMMITDATE']:
        supp_key = lineitem['L_SUPPKEY']
        if supp_key not in late_orders_by_supplier:
            late_orders_by_supplier[supp_key] = []
        late_orders_by_supplier[supp_key].append(lineitem['L_ORDERKEY'])

# Determine suppliers who are the ONLY one late to deliver on multi-supplier orders.
result = []
for supp_key in late_orders_by_supplier:
    late_orders = set(late_orders_by_supplier[supp_key])
    for order_key in late_orders:
        other_lineitems = mongo_db.lineitem.find({"L_ORDERKEY": order_key})
        other_suppliers = {item['L_SUPPKEY'] for item in other_lineitems if item['L_SUPPKEY'] != supp_key}
        if all(supplier not in late_orders_by_supplier or order_key not in late_orders_by_supplier[supplier]
               for supplier in other_suppliers):
            result.append((supp_key, suppliers[supp_key]['S_NAME'], suppliers[supp_key]['S_ADDRESS']))

# Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(['S_SUPPKEY', 'S_NAME', 'S_ADDRESS'])
    for row in result:
        csv_writer.writerow(row)

# Close the connections
mysql_conn.close()
mongo_client.close()
