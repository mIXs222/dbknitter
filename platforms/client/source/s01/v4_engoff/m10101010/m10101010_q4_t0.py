import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch')
mysql_cursor = mysql_connection.cursor()

# Get the necessary data from MySQL lineitem table
mysql_query = """
    SELECT L_ORDERKEY
    FROM lineitem
    WHERE L_COMMITDATE < L_RECEIPTDATE
"""
mysql_cursor.execute(mysql_query)
lineitem_data = mysql_cursor.fetchall()

# Filter the order keys that have a late receipt date based on the retrieved data
late_order_keys = set([order[0] for order in lineitem_data])

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client.tpch
orders = mongodb.orders

# Define the date range for orders
start_date = "1993-07-01"
end_date = "1993-10-01"

# Query MongoDB to get order priority counts for orders in the given date range
mongo_query = {
    "O_ORDERDATE": {"$gte": start_date, "$lt": end_date},
    "O_ORDERKEY": {"$in": list(late_order_keys)}
}
order_priority_count = orders.aggregate([
    {"$match": mongo_query},
    {"$group": {
        "_id": "$O_ORDERPRIORITY",
        "count": {"$sum": 1}
    }},
    {"$sort": {"_id": 1}}
])

# Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(['O_ORDERPRIORITY', 'order_count'])

    for doc in order_priority_count:
        csv_writer.writerow([doc['_id'], doc['count']])

# Close connections
mysql_cursor.close()
mysql_connection.close()
mongo_client.close()
