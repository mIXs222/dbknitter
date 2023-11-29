import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_coll = mongo_db['customer']

# SQL query to count the number of orders per customer in MySQL (excluding certain order comments)
order_query = """
    SELECT O_CUSTKEY, COUNT(*) as order_count
    FROM orders
    WHERE O_COMMENT NOT LIKE '%pending%deposits%'
    GROUP BY O_CUSTKEY
"""

# Execute the MySQL query
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute(order_query)
orders_per_customer = dict(mysql_cursor.fetchall())

# Retrieve customers' list from MongoDB
customer_list = mongo_coll.find({}, {'C_CUSTKEY': 1})

# Final dictionary to map the number of orders to the number of customers with that order count
order_distribution = {}

# Loop over customers from MongoDB and count their orders from MySQL results
for cust in customer_list:
    custkey = cust['C_CUSTKEY']
    order_count = orders_per_customer.get(custkey, 0)
    order_distribution[order_count] = order_distribution.get(order_count, 0) + 1

# Write the output to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['num_orders', 'num_customers'])
    for num_orders, num_customers in order_distribution.items():
        writer.writerow([num_orders, num_customers])

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
