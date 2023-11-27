import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4'
)

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Define the query for MySQL
mysql_query = """
    SELECT 
        L_ORDERKEY 
    FROM 
        lineitem 
    WHERE 
        L_RECEIPTDATE > L_COMMITDATE;
"""

# Execute MySQL query
order_keys_with_late_lineitems = set()
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    for row in cursor.fetchall():
        order_keys_with_late_lineitems.add(row[0])
mysql_conn.close()

# MongoDB query
mongodb_query = {
    'O_ORDERDATE': {'$gte': '1993-07-01', '$lt': '1993-10-01'},
    'O_ORDERKEY': {'$in': list(order_keys_with_late_lineitems)}
}

# Execute MongoDB query
orders_with_late_lineitems = mongodb_db.orders.find(mongodb_query, {'_id': 0, 'O_ORDERPRIORITY': 1})

# Aggregate results
priority_counts = {}
for order in orders_with_late_lineitems:
    priority = order['O_ORDERPRIORITY']
    priority_counts[priority] = priority_counts.get(priority, 0) + 1

# Sort the results
sorted_priority_counts = sorted(priority_counts.items())

# Write to CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['O_ORDERPRIORITY', 'order_count'])
    for priority, count in sorted_priority_counts:
        writer.writerow([priority, count])
