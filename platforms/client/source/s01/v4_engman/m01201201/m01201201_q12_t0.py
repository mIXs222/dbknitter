import pymysql
import pymongo
import csv
from datetime import datetime


# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_collection = mongo_db['lineitem']

# Query line items from MongoDB
mongodb_query = {
    "L_RECEIPTDATE": {"$gte": datetime(1994, 1, 1), "$lt": datetime(1995, 1, 1)},
    "L_COMMITDATE": {"$lt": "$L_RECEIPTDATE"},
    "L_SHIPDATE": {"$lt": "$L_COMMITDATE"},
    "L_SHIPMODE": {"$in": ["MAIL", "SHIP"]}
}
lineitems = list(mongo_collection.find(mongodb_query, projection={'_id': False}))

# Retrieve order priorities from MySQL
orderkey_priority = {}
orderkeys = [li['L_ORDERKEY'] for li in lineitems]
placeholders = ','.join(['%s'] * len(orderkeys))
mysql_query = f"""
    SELECT O_ORDERKEY, O_ORDERPRIORITY
    FROM orders
    WHERE O_ORDERKEY IN ({placeholders})
"""
mysql_cursor.execute(mysql_query, orderkeys)
for orderkey, priority in mysql_cursor.fetchall():
    orderkey_priority[orderkey] = priority

# Combine results
results = {}
for li in lineitems:
    shipmode = li['L_SHIPMODE']
    order_priority = orderkey_priority.get(li['L_ORDERKEY'], None)
    
    if order_priority:
        priority_group = 'HIGH' if order_priority in ['URGENT', 'HIGH'] else 'LOW'
        if shipmode not in results:
            results[shipmode] = {'HIGH': 0, 'LOW': 0}
        results[shipmode][priority_group] += 1

# Write results to CSV
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['L_SHIPMODE', 'HIGH_PRIORITY_COUNT', 'LOW_PRIORITY_COUNT'])
    for shipmode in sorted(results):
        writer.writerow([shipmode, results[shipmode]['HIGH'], results[shipmode]['LOW']])

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
