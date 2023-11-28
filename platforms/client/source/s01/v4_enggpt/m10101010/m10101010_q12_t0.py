import csv
import pymysql
import pymongo
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
orders_collection = mongo_db.orders

# Extract necessary 'orders' from MongoDB
high_priority = ['1-URGENT', '2-HIGH']
start_date = datetime(1994, 1, 1)
end_date = datetime(1994, 12, 31)
orders = list(orders_collection.find(
    {'$and': [
        {'O_ORDERDATE': {'$gte': start_date, '$lte': end_date}},
        {'O_ORDERPRIORITY': {'$in': high_priority + ['OTHER']}}
    ]},
    {'_id': 0, 'O_ORDERKEY': 1, 'O_ORDERPRIORITY': 1}
))

# Convert MongoDB orders to a format compatible with MySQL
orders_map = {}
for order in orders:
    key = order['O_ORDERKEY']
    priority = 'HIGH' if order['O_ORDERPRIORITY'] in high_priority else 'LOW'
    orders_map[key] = priority

# Query the 'lineitem' table in MySQL
shipping_modes = ['MAIL', 'SHIP']
results = []
for mode in shipping_modes:
    mysql_cursor.execute("""
        SELECT L_SHIPMODE, COUNT(*) as count, %s as priority
        FROM lineitem 
        WHERE L_SHIPMODE = %s 
        AND L_SHIPDATE < L_COMMITDATE 
        AND L_COMMITDATE < L_RECEIPTDATE 
        AND L_RECEIPTDATE BETWEEN %s AND %s
        GROUP BY L_SHIPMODE
    """, (mode, mode, start_date, end_date))
    mode_counts = {'HIGH_LINE_COUNT': 0, 'LOW_LINE_COUNT': 0}
    for row in mysql_cursor.fetchall():
        order_key = row[0]
        count = row[1]
        priority = orders_map.get(order_key, 'LOW')
        line_count_key = f"{priority}_LINE_COUNT"
        mode_counts[line_count_key] += count
    results.append((mode, mode_counts['HIGH_LINE_COUNT'], mode_counts['LOW_LINE_COUNT']))

# Write the query results to a CSV file
with open('query_output.csv', mode='w') as file:
    writer = csv.writer(file)
    writer.writerow(['SHIPMODE', 'HIGH_LINE_COUNT', 'LOW_LINE_COUNT'])
    for row in results:
        writer.writerow(row)

# Closing connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
