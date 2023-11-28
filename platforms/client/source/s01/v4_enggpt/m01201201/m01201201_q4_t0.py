import csv
import pymysql
import pymongo
from datetime import datetime
from pymongo import MongoClient

# Function to query MySQL database
def query_mysql():
    connection = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 database='tpch',
                                 cursorclass=pymysql.cursors.Cursor)
    try:
        with connection.cursor() as cursor:
            sql = """
            SELECT O_ORDERPRIORITY, O_ORDERKEY
            FROM orders
            WHERE O_ORDERDATE >= %s AND O_ORDERDATE <= %s
            """
            cursor.execute(sql, ('1993-07-01', '1993-10-01'))
            return cursor.fetchall()
    finally:
        connection.close()

# Function to query MongoDB database
def query_mongodb(orderkeys):
    client = MongoClient('mongodb', 27017)
    db = client.tpch
    lineitem_collection = db.lineitem
    qualifying_documents = lineitem_collection.find(
        {
            'L_ORDERKEY': {'$in': orderkeys},
            'L_COMMITDATE': {'$lt': '$L_RECEIPTDATE'}
        },
        {'L_ORDERKEY': 1}
    )
    return [doc['L_ORDERKEY'] for doc in qualifying_documents]

# Query MySQL and get valid orderkeys with their priorities
mysql_result = query_mysql()
orderkeys_with_priority = {order[1]: order[0] for order in mysql_result}

# Filter orderkeys based on lineitem criteria from MongoDB
orderkeys = list(orderkeys_with_priority.keys())
mongodb_orderkeys = set(query_mongodb(orderkeys))

# Filtering MySQL results based on MongoDB results
filtered_order_priorities = [
    (orderkeys_with_priority[ok], ok) for ok in mongodb_orderkeys
]

# Count the qualifying orders by priority
priority_count = {}
for priority, orderkey in filtered_order_priorities:
    if priority in priority_count:
        priority_count[priority] += 1
    else:
        priority_count[priority] = 1

# Sort the results by order priority
sorted_results = sorted(priority_count.items(), key=lambda x: x[0])

# Write the output to a CSV file
with open('query_output.csv', mode='w') as file:
    writer = csv.writer(file)
    writer.writerow(['O_ORDERPRIORITY', 'ORDER_COUNT'])
    for result in sorted_results:
        writer.writerow(list(result))
