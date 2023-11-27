import pymysql
import pymongo
import csv
from datetime import datetime

# Function to get MySQL connection
def get_mysql_connection():
    return pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        db='tpch'
    )

# Function to get Mongo DB connection
def get_mongo_connection():
    return pymongo.MongoClient('mongodb', 27017).tpch

# Function to fetch orders which have the given priorities
def fetch_orders(mysql_conn, priorities):
    orders = {}
    with mysql_conn.cursor() as cursor:
        query = """
            SELECT O_ORDERKEY, O_ORDERPRIORITY
            FROM orders
            WHERE O_ORDERDATE BETWEEN '1993-07-01' AND '1993-10-01'
            AND O_ORDERPRIORITY IN (%s)
        """
        in_p = ', '.join(['%s'] * len(priorities))
        cursor.execute(query % in_p, priorities)
        for row in cursor.fetchall():
            orders[row[0]] = row[1]
    return orders

# Function to fetch lineitem data from MongoDB
def fetch_lineitems(mongo_db, order_keys):
    lineitems = []
    for doc in mongo_db.lineitem.find({'L_ORDERKEY': {'$in': order_keys}}):
        if doc['L_COMMITDATE'] < doc['L_RECEIPTDATE']:
            lineitems.append(doc['L_ORDERKEY'])
    return set(lineitems)

try:
    # Connect to MySQL and MongoDB
    mysql_conn = get_mysql_connection()
    mongo_db = get_mongo_connection()

    # Fetch all possible order priorities from the orders table
    order_priorities = sorted(fetch_orders(mysql_conn, []))
    # Fetch orders within the required date range and have the fetched priorities
    valid_orders = fetch_orders(mysql_conn, order_priorities.keys())

    # Fetch lineitems that are related to the valid orders
    late_lineitems_orders = fetch_lineitems(mongo_db, list(valid_orders.keys()))

    # Count the number of late orders per order priority
    order_priority_count = {priority: 0 for priority in order_priorities}
    for order_key in late_lineitems_orders:
        if order_key in valid_orders:
            priority = valid_orders[order_key]
            order_priority_count[priority] += 1

    # Write the query output to a CSV file
    with open('query_output.csv', 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['O_ORDERPRIORITY', 'order_count'])
        for priority, count in sorted(order_priority_count.items()):
            writer.writerow([priority, count])

finally:
    # Close both MySQL and MongoDB connections
    if mysql_conn.open:
        mysql_conn.close()

