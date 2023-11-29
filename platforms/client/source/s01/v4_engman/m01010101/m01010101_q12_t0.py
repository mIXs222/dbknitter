# query.py

import pymysql
import pymongo
import csv
import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Query MySQL for orders of high priority
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT O_ORDERKEY, O_ORDERPRIORITY
        FROM orders
        WHERE O_ORDERPRIORITY = '1-URGENT' OR O_ORDERPRIORITY = '2-HIGH'
    """)
    high_priority_orders = cursor.fetchall()

high_priority_orderkeys = [order[0] for order in high_priority_orders if order[1] in ['1-URGENT', '2-HIGH']]

# Query MongoDB for lineitem details
lineitems = mongodb.lineitem.find({
    'L_RECEIPTDATE': {'$gte': datetime.datetime(1994, 1, 1), '$lt': datetime.datetime(1995, 1, 1)},
    'L_COMMITDATE': {'$lt': '$L_RECEIPTDATE'},
    'L_SHIPMODE': {'$in': ['MAIL', 'SHIP']},
    'L_SHIPDATE': {'$lt': '$L_COMMITDATE'}
})

# Processing results
results = {}
for lineitem in lineitems:
    if lineitem['L_ORDERKEY'] in high_priority_orderkeys:
        priority = 'High'
    else:
        priority = 'Low'

    ship_mode = lineitem['L_SHIPMODE']
    if ship_mode not in results:
        results[ship_mode] = {'High': 0, 'Low': 0}
    results[ship_mode][priority] += 1

# Writing results to CSV
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['SHIPMODE', 'HIGH_PRIORITY_COUNT', 'LOW_PRIORITY_COUNT'])

    for ship_mode, counts in sorted(results.items()):
        writer.writerow([ship_mode, counts['High'], counts['Low']])

# Close connections
mysql_conn.close()
mongo_client.close()
