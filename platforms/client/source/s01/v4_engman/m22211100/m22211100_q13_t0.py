import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Get a list of customer IDs from MongoDB
customer_ids = mongo_db.customer.find({}, {"C_CUSTKEY": 1, "_id": 0})
customer_id_list = [cust_doc['C_CUSTKEY'] for cust_doc in customer_ids]

# Get order information from MySQL
with mysql_connection.cursor() as cursor:
    order_query = """
        SELECT O_CUSTKEY, COUNT(*) as order_count
        FROM orders
        WHERE O_COMMENT NOT LIKE '%pending%deposits%'
        GROUP BY O_CUSTKEY;
    """
    cursor.execute(order_query)
    orders_by_customer = cursor.fetchall()

# Process results for output
order_counts = {}
for (cust_key, order_count) in orders_by_customer:
    if cust_key in customer_id_list:
        order_counts.setdefault(order_count, 0)
        order_counts[order_count] += 1

# Write results to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['Number of Orders', 'Number of Customers'])
    for num_orders, num_customers in order_counts.items():
        csvwriter.writerow([num_orders, num_customers])

# Close the MySQL connection
mysql_connection.close()
