# python_code.py

import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
orders_collection = mongodb_db['orders']

# Retrieve customer data from MySQL
mysql_cursor.execute("SELECT C_CUSTKEY, C_NAME FROM customer")
customers = mysql_cursor.fetchall()

# Retrieve orders data from MongoDB considering NOT LIKE condition
orders = list(orders_collection.find(
    {"O_COMMENT": {"$not": {"$regex": "pendingdeposits"}}},
    {"O_ORDERKEY": 1, "O_CUSTKEY": 1}
))

# Prepare data structures for counts
customer_order_counts = {}
for customer in customers:
    # Initialize counts to 0
    customer_order_counts[customer[0]] = 0

for order in orders:
    # Increment count for each customer found in orders data
    if order['O_CUSTKEY'] in customer_order_counts:
        customer_order_counts[order['O_CUSTKEY']] += 1

# Group by counts to get distributions
distribution = {}
for count in customer_order_counts.values():
    distribution.setdefault(count, 0)
    distribution[count] += 1

# Write the results to query_output.csv
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['C_COUNT', 'CUSTDIST'])
    for count, custdist in sorted(distribution.items(), key=lambda item: (item[1], item[0]), reverse=True):
        writer.writerow([count, custdist])

# Close the database connections
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()
