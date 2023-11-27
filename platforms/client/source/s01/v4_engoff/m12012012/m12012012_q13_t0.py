import pymysql
import pymongo
import csv

# Function to connect to MySQL and retrieve customers
def get_customers_from_mysql():
    connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
    try:
        with connection.cursor() as cursor:
            sql = "SELECT C_CUSTKEY FROM customer"
            cursor.execute(sql)
            customers = cursor.fetchall()
            return [c[0] for c in customers]
    finally:
        connection.close()

# Function to connect to MongoDB and retrieve orders
def get_orders_from_mongodb():
    client = pymongo.MongoClient("mongodb", 27017)
    db = client["tpch"]
    
    orders = db.orders.find({"$and": [{"O_ORDERSTATUS": {"$nin": ["pending", "deposits"]}}, {"O_COMMENT": {"$not": {"$regex": "pending|deposits"}}}]})
    order_count_per_customer = {}
    
    for order in orders:
        cust_key = order["O_CUSTKEY"]
        order_count_per_customer[cust_key] = order_count_per_customer.get(cust_key, 0) + 1
        
    return order_count_per_customer

# Retrieve data from databases
mysql_customers = get_customers_from_mysql()
mongodb_orders = get_orders_from_mongodb()

# Combine and process the data
combined_data = {}

# Initialize all customers with zero orders
for customer_key in mysql_customers:
    combined_data[customer_key] = 0

# Update order counts for customers who have orders
for customer_key, order_count in mongodb_orders.items():
    combined_data[customer_key] = order_count

# Count the distribution
distribution = {}
for order_count in combined_data.values():
    distribution[order_count] = distribution.get(order_count, 0) + 1

# Output the data to the CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Number of Orders', 'Number of Customers'])
    for order_count, customer_count in sorted(distribution.items()):
        writer.writerow([order_count, customer_count])
