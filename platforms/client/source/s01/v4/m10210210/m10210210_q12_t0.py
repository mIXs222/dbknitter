# query_code.py
import pymysql
import pymongo
import csv
from datetime import datetime

# Connection to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connection to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
orders_collection = mongo_db['orders']

# MongoDB query to get order keys and priorities for orders that are MAIL or SHIP
mongo_query = {
    'O_ORDERDATE': {'$gte': datetime(1994, 1, 1), '$lt': datetime(1995, 1, 1)},
    'O_ORDERPRIORITY': {'$in': ['1-URGENT', '2-HIGH']}
}
projection = {'O_ORDERKEY': 1, 'O_ORDERPRIORITY': 1}
mongo_orders = list(orders_collection.find(mongo_query, projection))

# Prepare the order keys with high priority
high_priority_order_keys = set()
for order in mongo_orders:
    high_priority_order_keys.add(order['O_ORDERKEY'])

# Construct the MySQL query
mysql_query = """
SELECT
    L_SHIPMODE,
    SUM(CASE
            WHEN L_ORDERKEY IN %s THEN 1
            ELSE 0
    END) AS HIGH_LINE_COUNT,
    SUM(CASE
            WHEN L_ORDERKEY NOT IN %s THEN 1
            ELSE 0
    END) AS LOW_LINE_COUNT
FROM
    lineitem
WHERE
    L_SHIPMODE IN ('MAIL', 'SHIP')
    AND L_COMMITDATE < L_RECEIPTDATE
    AND L_SHIPDATE < L_COMMITDATE
    AND L_RECEIPTDATE >= '1994-01-01'
    AND L_RECEIPTDATE < '1995-01-01'
GROUP BY
    L_SHIPMODE
ORDER BY
    L_SHIPMODE
"""

# Execute MySQL query
mysql_cursor.execute(mysql_query, (high_priority_order_keys, high_priority_order_keys))
results = mysql_cursor.fetchall()

# Write results to CSV
with open('query_output.csv', mode='w', newline='') as file:
    csv_writer = csv.writer(file)
    csv_writer.writerow(['L_SHIPMODE', 'HIGH_LINE_COUNT', 'LOW_LINE_COUNT'])
    for row in results:
        csv_writer.writerow(row)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
