import pymysql
import pymongo
import csv

# Establish connection to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Initialize MySQL cursor
mysql_cursor = mysql_conn.cursor()

# Establish connection to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
lineitem_collection = mongodb_db['lineitem']

# MongoDB query
mongodb_query = {
    "L_COMMITDATE": {"$lt": "$L_RECEIPTDATE"}
}

# Finding all order keys in lineitem collection that match the condition
lineitem_orderkeys = [
    doc['L_ORDERKEY'] for doc in lineitem_collection.find(
        mongodb_query, {'L_ORDERKEY': 1, '_id': 0}
    )
]

# Formatting the list of order keys for MySQL IN clause
format_strings = ','.join(['%s'] * len(lineitem_orderkeys))

# MySQL query
mysql_query = f"""
SELECT O_ORDERPRIORITY, COUNT(*) AS ORDER_COUNT
FROM orders
WHERE O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-10-01'
AND O_ORDERKEY IN ({format_strings})
GROUP BY O_ORDERPRIORITY
ORDER BY O_ORDERPRIORITY;
"""

# Execute MySQL query with the list of matching order keys
mysql_cursor.execute(mysql_query, tuple(lineitem_orderkeys))

# Fetch all rows from MySQL query
results = mysql_cursor.fetchall()

# Write results to CSV
with open('query_output.csv', 'w') as file:
    writer = csv.writer(file)
    writer.writerow(['O_ORDERPRIORITY', 'ORDER_COUNT'])
    for result in results:
        writer.writerow(result)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()
