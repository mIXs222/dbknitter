import csv
import pymysql
import pymongo
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

mysql_query = """
SELECT O_ORDERPRIORITY, COUNT(DISTINCT O_ORDERKEY) AS ORDER_COUNT
FROM orders
WHERE O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-10-01'
AND O_ORDERKEY IN (
    SELECT L_ORDERKEY
    FROM lineitem
    WHERE L_RECEIPTDATE > L_COMMITDATE
)
GROUP BY O_ORDERPRIORITY
ORDER BY O_ORDERPRIORITY ASC;
"""

try:
    mysql_cursor.execute(mysql_query)
    mysql_orders_data = mysql_cursor.fetchall()
finally:
    mysql_conn.close()

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
lineitem_collection = mongodb_db['lineitem']

mongodb_orders = lineitem_collection.aggregate([
    {"$match": {"L_RECEIPTDATE": {"$gt": "L_COMMITDATE"}}},
    {
        "$lookup": {
            "from": "orders",
            "localField": "L_ORDERKEY",
            "foreignField": "O_ORDERKEY",
            "as": "order_info"
        }
    },
    {"$unwind": "$order_info"},
    {"$match": {"order_info.O_ORDERDATE": {"$gte": datetime(1993, 7, 1), "$lt": datetime(1993, 10, 1)}}},
    {"$group": {"_id": "$order_info.O_ORDERPRIORITY", "ORDER_COUNT": {"$sum": 1}}},
    {"$sort": {"_id": 1}}
])

final_results = {priority: count for priority, count in mysql_orders_data}
for doc in mongodb_orders:
    if doc["_id"] in final_results:
        final_results[doc["_id"]] += doc["ORDER_COUNT"]
    else:
        final_results[doc["_id"]] = doc["ORDER_COUNT"]

# Writing results to query_output.csv
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['O_ORDERPRIORITY', 'ORDER_COUNT']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for priority in sorted(final_results.keys()):
        writer.writerow({'O_ORDERPRIORITY': priority, 'ORDER_COUNT': final_results[priority]})
