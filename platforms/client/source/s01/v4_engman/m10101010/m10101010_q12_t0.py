import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to MySQL using pymysql
mysql_connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_connection.cursor()

# Connect to MongoDB using pymongo
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
orders_collection = mongodb['orders']

# Fetch high order priority lineitems from MySQL
mysql_high_query = """
    SELECT L_SHIPMODE, COUNT(*)
    FROM lineitem
    WHERE L_SHIPMODE IN ('MAIL', 'SHIP')
        AND L_RECEIPTDATE BETWEEN '1994-01-01' AND '1995-01-01'
        AND L_COMMITDATE < L_RECEIPTDATE
        AND L_SHIPDATE < L_COMMITDATE
    GROUP BY L_SHIPMODE
"""
mysql_cursor.execute(mysql_high_query)
high_priority_result = mysql_cursor.fetchall()

# Fetch order priorities from MongoDB
mongo_orders = list(orders_collection.find({
    "O_ORDERDATE": {"$gte": datetime(1994, 1, 1), "$lt": datetime(1995, 1, 1)},
    "O_ORDERPRIORITY": {"$in": ["URGENT", "HIGH"]}
}, {"O_ORDERKEY": 1, "O_ORDERPRIORITY": 1}))

# Mapping of order keys to order priorities from MongoDB
order_priorities = {order['O_ORDERKEY']: order['O_ORDERPRIORITY'] for order in mongo_orders}

# Fetch low order priority lineitems from MySQL
mysql_low_query = """
    SELECT L_SHIPMODE, COUNT(*)
    FROM lineitem
    WHERE L_SHIPMODE IN ('MAIL', 'SHIP')
        AND L_RECEIPTDATE BETWEEN '1994-01-01' AND '1995-01-01'
        AND L_COMMITDATE < L_RECEIPTDATE
        AND L_SHIPDATE < L_COMMITDATE
    GROUP BY L_SHIPMODE
"""
mysql_cursor.execute(mysql_low_query)
low_priority_result = mysql_cursor.fetchall()

# Combining the results for high and low priority lineitems
combined_results = {}

for ship_mode, count in high_priority_result:
    if ship_mode not in combined_results:
        combined_results[ship_mode] = {'HIGH': 0, 'LOW': 0}
    # Only aggregate if the lineitem order key is in high priorities
    combined_results[ship_mode]['HIGH'] += count

for ship_mode, count in low_priority_result:
    if ship_mode not in combined_results:
        combined_results[ship_mode] = {'HIGH': 0, 'LOW': 0}
    combined_results[ship_mode]['LOW'] += count

# Write the results to query_output.csv
with open('query_output.csv', mode='w', newline='') as csv_file:
    field_names = ['SHIPMODE', 'HIGH_PRIORITY_COUNT', 'LOW_PRIORITY_COUNT']
    writer = csv.DictWriter(csv_file, fieldnames=field_names)
    
    writer.writeheader()
    for ship_mode, counts in sorted(combined_results.items()):
        row = {
            'SHIPMODE': ship_mode,
            'HIGH_PRIORITY_COUNT': counts['HIGH'],
            'LOW_PRIORITY_COUNT': counts['LOW']
        }
        writer.writerow(row)

# Closing database connections
mysql_cursor.close()
mysql_connection.close()
mongo_client.close()
