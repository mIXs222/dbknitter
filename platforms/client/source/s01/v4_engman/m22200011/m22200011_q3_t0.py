import pymysql
import pymongo
import csv
from datetime import datetime

# Establish connection to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Establish connection to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
orders_collection = mongo_db['orders']
lineitem_collection = mongo_db['lineitem']

# Fetch necessary data from MySQL
customer_query = """
SELECT C_CUSTKEY, C_MKTSEGMENT
FROM customer
WHERE C_MKTSEGMENT = 'BUILDING'
"""
mysql_cursor.execute(customer_query)
building_customers = {row[0]: row[1] for row in mysql_cursor.fetchall()}

# Filtering the orders in MongoDB
orders_query = {
    'O_ORDERDATE': {'$lt': datetime(1995, 3, 5)},
    'O_CUSTKEY': {'$in': list(building_customers.keys())}
}
orders = list(orders_collection.find(orders_query, {'_id': 0, 'O_ORDERKEY': 1, 'O_ORDERDATE': 1, 'O_SHIPPRIORITY': 1}))

# Filtering relevant orders in lineitem MongoDB
lineitem_query = {
    'L_SHIPDATE': {'$gt': datetime(1995, 3, 15)},
    'L_ORDERKEY': {'$in': [order['O_ORDERKEY'] for order in orders]}
}
lineitems = list(lineitem_collection.find(lineitem_query, {'_id': 0, 'L_ORDERKEY': 1, 'L_EXTENDEDPRICE': 1, 'L_DISCOUNT': 1}))

# Generate report
output_data = []
for order in orders:
    order_revenue = sum(
        li['L_EXTENDEDPRICE'] * (1 - li['L_DISCOUNT'])
        for li in lineitems if li['L_ORDERKEY'] == order['O_ORDERKEY']
    )
    output_data.append([
        order['O_ORDERKEY'],
        order_revenue,
        order['O_ORDERDATE'].strftime('%Y-%m-%d'),
        order['O_SHIPPRIORITY']
    ])

# Sort the result by revenue in decreasing order
output_data.sort(key=lambda x: x[1], reverse=True)

# Write result to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['O_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY'])
    for row in output_data:
        writer.writerow(row)

# Close all connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
