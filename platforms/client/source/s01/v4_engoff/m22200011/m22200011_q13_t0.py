# query.py
import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    cursorclass=pymysql.cursors.Cursor
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
orders_collection = mongo_db['orders']

# Fetch customer data from MySQL
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute("SELECT C_CUSTKEY FROM customer")
customers = {row[0] for row in mysql_cursor.fetchall()}

# Close MySQL cursor and connection
mysql_cursor.close()
mysql_conn.close()

# Count orders made by customers from MongoDB
customer_order_counts = {cust_key: 0 for cust_key in customers}
for order in orders_collection.find(
        {"$and": [{"O_ORDERSTATUS": {"$nin": ["pending", "deposits"]}},
                  {"O_COMMENT": {"$nin": ["/.*pending.*/i", "/.*deposits.*/i"]}}]}):
    cust_key = order['O_CUSTKEY']
    if cust_key in customer_order_counts:
        customer_order_counts[cust_key] += 1

# Combine results and write to query_output.csv
distribution_of_customers = {}
for count in customer_order_counts.values():
    distribution_of_customers[count] = distribution_of_customers.get(count, 0) + 1

with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['Number of Orders', 'Count of Customers'])
    for number_of_orders, count_of_customers in sorted(distribution_of_customers.items()):
        csvwriter.writerow([number_of_orders, count_of_customers])
