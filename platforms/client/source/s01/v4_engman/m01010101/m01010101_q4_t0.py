# query.py
import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Fetch orders between given dates from MySQL
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute("""
SELECT O_ORDERKEY, O_ORDERPRIORITY
FROM orders
WHERE O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-10-01'
""")

# Get order keys and priority in a dict with order key as the key
orders_with_priority = {}
for order in mysql_cursor:
    orders_with_priority[order[0]] = order[1]

# Query MongoDB lineitem collection
lineitem_collection = mongodb_db['lineitem']
lineitems = lineitem_collection.find({
    'L_RECEIPTDATE': {'$gt': 'L_COMMITDATE'}
})

# Filter lineitems by order keys from MySQL and count orders
order_counts = {}
for lineitem in lineitems:
    order_key = int(lineitem['L_ORDERKEY'])
    if order_key in orders_with_priority:
        order_priority = orders_with_priority[order_key]
        if order_priority in order_counts:
            order_counts[order_priority] += 1
        else:
            order_counts[order_priority] = 1

# Sort by order priority
sorted_order_counts = sorted(order_counts.items(), key=lambda x: x[0])

# Output results to CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['O_ORDERPRIORITY', 'ORDER_COUNT'])
    for order_priority, count in sorted_order_counts:
        writer.writerow([order_priority, count])

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()
