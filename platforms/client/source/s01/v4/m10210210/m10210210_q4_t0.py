import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Get all orders from MongoDB that match the date criteria
orders_collection = mongodb['orders']
mongo_orders = list(orders_collection.find({
    "O_ORDERDATE": {
        "$gte": '1993-07-01',
        "$lt": '1993-10-01'
    }
}, {"O_ORDERKEY": 1, "O_ORDERPRIORITY": 1}))

# Dictionary to hold the final results
results = {}

# MySQL lineitem subquery
with mysql_connection.cursor() as cursor:
    for order in mongo_orders:
        cursor.execute(
            "SELECT L_ORDERKEY FROM lineitem WHERE L_ORDERKEY = %s AND L_COMMITDATE < L_RECEIPTDATE",
            (order['O_ORDERKEY'],)
        )
        # If the order key exists in lineitem
        if cursor.fetchone():
            order_priority = order['O_ORDERPRIORITY']
            results[order_priority] = results.get(order_priority, 0) + 1

# Writing query results to query_output.csv
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['O_ORDERPRIORITY', 'ORDER_COUNT'])
    for priority, count in sorted(results.items()):
        writer.writerow([priority, count])

# Close the database connections
mysql_connection.close()
mongo_client.close()
