import pymysql
import pymongo
import csv

# MySQL connection setup
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# MongoDB connection setup
client = pymongo.MongoClient('mongodb', 27017)
mongodb = client.tpch

# Query to get relevant orders from MySQL
mysql_query = """
SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERDATE, O_TOTALPRICE
FROM orders
HAVING SUM(L_QUANTITY) > 300
"""

# Execute MySQL query
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute(mysql_query)
orders_data = mysql_cursor.fetchall()

# Collecting order keys for MongoDB query
order_keys = [order[0] for order in orders_data]

# Query to get items from MongoDB
lineitems = mongodb.lineitem.aggregate([
    {"$match": {"L_ORDERKEY": {"$in": order_keys}}},
    {"$group": {"_id": "$L_ORDERKEY", "total_quantity": {"$sum": "$L_QUANTITY"}}},
    {"$match": {"total_quantity": {"$gt": 300}}}
])
lineitem_dict = {item['_id']: item['total_quantity'] for item in lineitems}

# Filter orders data based on the quantity from lineitems
filtered_orders_data = [order for order in orders_data if order[0] in lineitem_dict]

# Get customer details from MongoDB
customer_keys = [order[1] for order in filtered_orders_data]
customers = mongodb.customer.find({"C_CUSTKEY": {"$in": customer_keys}})
customer_dict = {customer['C_CUSTKEY']: customer['C_NAME'] for customer in customers}

# Combine data and sort as required
combined_data = [
    (customer_dict[order[1]], order[1], order[0], order[2], order[3], lineitem_dict[order[0]])
    for order in filtered_orders_data
]
combined_data.sort(key=lambda x: (-x[4], x[3]))  # Sorting by O_TOTALPRICE DESC, O_ORDERDATE ASC

# Write to csv
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'TOTAL_QUANTITY'])
    csvwriter.writerows(combined_data)

# Close connections
mysql_cursor.close()
mysql_conn.close()
client.close()
