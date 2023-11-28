import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Start with the mongodb orders that fall within the given timeframe
start_time = datetime(1993, 7, 1)
end_time = datetime(1993, 10, 1)
orders = list(mongodb_db.orders.find({
    "O_ORDERDATE": {"$gte": start_time, "$lt": end_time}
}, {"_id": 0, "O_ORDERKEY": 1, "O_ORDERPRIORITY": 1}))

# Filter the orders based on the MySQL lineitem commit and receipt dates
with mysql_conn.cursor() as cursor:
    order_keys = [order["O_ORDERKEY"] for order in orders]
    format_strings = ','.join(['%s'] * len(order_keys))
    cursor.execute(f"""
        SELECT L_ORDERKEY
        FROM lineitem
        WHERE L_COMMITDATE < L_RECEIPTDATE AND L_ORDERKEY IN ({format_strings})
    """, tuple(order_keys))
    valid_order_keys = [row[0] for row in cursor.fetchall()]

# Extract final order priorities and counts
result = {}
for order in orders:
    if order['O_ORDERKEY'] in valid_order_keys:
        priority = order['O_ORDERPRIORITY']
        if priority not in result:
            result[priority] = 0
        result[priority] += 1

# Sort results by order priority
sorted_result = sorted(result.items())

# Write to CSV
with open('query_output.csv', 'w', newline='') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(['O_ORDERPRIORITY', 'COUNT'])
    for priority, count in sorted_result:
        writer.writerow([priority, count])

# Close connections
mysql_conn.close()
mongodb_client.close()
