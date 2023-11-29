import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
orders_collection = mongodb_db['orders']

# Query MySQL for customer market segment and join with lineitem
mysql_cursor.execute("""
SELECT c.C_CUSTKEY, l.L_ORDERKEY, l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT) AS REVENUE, l.L_SHIPDATE
FROM customer c JOIN lineitem l ON c.C_CUSTKEY = l.L_PARTKEY
WHERE c.C_MKTSEGMENT = 'BUILDING'
""")
mysql_results = mysql_cursor.fetchall()

# Filter orders in MongoDB
mongodb_orders = list(orders_collection.find(
    {'O_ORDERDATE': {'$lt': '1995-03-05'}, 'O_ORDERKEY': {'$in': [row[1] for row in mysql_results]}}
))

# Combine results
final_results = []
for order in mongodb_orders:
    order_key = order['O_ORDERKEY']
    ship_priority = order.get('O_SHIPPRIORITY', None)
    for row in mysql_results:
        if row[1] == order_key and row[3] > '1995-03-15':
            final_results.append((order_key, row[2], order['O_ORDERDATE'], ship_priority))

# Sort results by revenue in descending order
final_results.sort(key=lambda x: x[1], reverse=True)

# Write to csv file
with open('query_output.csv', 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(['O_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY'])
    for result in final_results:
        csv_writer.writerow(result)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()
