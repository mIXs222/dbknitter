import pymysql
import pymongo
import csv

# Connecting to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Connecting to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_lineitem_collection = mongo_db['lineitem']

# MySQL query to fetch orders between given dates
mysql_query = """
SELECT O_ORDERPRIORITY, O_ORDERKEY FROM orders
WHERE O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE <= '1993-10-01';
"""
mysql_cursor.execute(mysql_query)

# Fetching all orders from MySQL
orders = mysql_cursor.fetchall()

# Filtering orders with at least one late lineitem
late_orders = []
for order_priority, order_key in orders:
    lineitems = mongo_lineitem_collection.find(
        {"L_ORDERKEY": order_key, "L_RECEIPTDATE": {"$gt": "L_COMMITDATE"}}
    )
    if lineitems.count() > 0:
        late_orders.append((order_priority, order_key))

# Counting orders for each priority
priority_counts = {}
for order_priority, _ in late_orders:
    if order_priority not in priority_counts:
        priority_counts[order_priority] = 0
    priority_counts[order_priority] += 1

# Sorting and writing to CSV
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['O_ORDERPRIORITY', 'ORDER_COUNT'])
    for order_priority in sorted(priority_counts):
        writer.writerow([order_priority, priority_counts[order_priority]])

# Closing connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
