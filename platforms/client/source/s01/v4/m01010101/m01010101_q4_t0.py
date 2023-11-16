import pymysql
import pymongo
import csv

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
lineitem_collection = mongodb_db.lineitem

# Query orders from MySQL
mysql_cursor.execute("""
    SELECT O_ORDERKEY, O_ORDERPRIORITY, O_ORDERDATE
    FROM orders
    WHERE O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-10-01'
""")
eligible_orders = {}
for order in mysql_cursor.fetchall():
    orderkey, orderpriority, _ = order
    eligible_orders[orderkey] = orderpriority

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Filter lineitems with commitment date before receipt date in MongoDB
result_structure = {}
for l_item in lineitem_collection.find({"L_COMMITDATE": {"$lt": "$L_RECEIPTDATE"}}):
    order_key = l_item.get('L_ORDERKEY')
    if order_key in eligible_orders:
        priority = eligible_orders[order_key]
        if priority not in result_structure:
            result_structure[priority] = 0
        result_structure[priority] += 1

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['O_ORDERPRIORITY', 'ORDER_COUNT']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    writer.writeheader()
    for order_priority in sorted(result_structure.keys()):
        writer.writerow({'O_ORDERPRIORITY': order_priority, 'ORDER_COUNT': result_structure[order_priority]})
