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
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client['tpch']
mongo_orders_collection = mongo_db['orders']

# Fetch customers from MySQL
mysql_cursor.execute("SELECT C_CUSTKEY FROM customer")
customers = mysql_cursor.fetchall()

# Process orders from MongoDB and count orders for each customer
cust_order_count = {}
for order_doc in mongo_orders_collection.find(
    {"$and": [{"O_COMMENT": {"$not": {"$regex": ".*pending.*deposits.*"}}},
              {"O_ORDERSTATUS": {"$nin": ["Pending", "Deposits"]}}]}
):
    cust_key = order_doc["O_CUSTKEY"]
    cust_order_count[cust_key] = cust_order_count.get(cust_key, 0) + 1

# Map order count to number of customers with that order count
order_count_distribution = {}
for customer in customers:
    cust_key = customer[0]
    count = cust_order_count.get(cust_key, 0)
    order_count_distribution[count] = order_count_distribution.get(count, 0) + 1

# Write results to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Number of Orders', 'Number of Customers'])
    for num_orders, num_customers in sorted(order_count_distribution.items()):
        writer.writerow([num_orders, num_customers])

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
