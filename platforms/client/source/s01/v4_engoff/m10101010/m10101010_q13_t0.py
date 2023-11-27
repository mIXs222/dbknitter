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

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Fetch customers from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT C_CUSTKEY FROM customer")
    mysql_customers = cursor.fetchall()

# Fetch orders from MongoDB
mongo_orders = mongodb['orders'].find(
    {"$and": [{"O_ORDERSTATUS": {"$ne": "PENDING"}}, {"O_ORDERSTATUS": {"$ne": "DEPOSITS"}}]},
    {"_id": 0, "O_CUSTKEY": 1}
)

# Create a dictionary to count orders by customer
order_count_dict = {str(customer[0]): 0 for customer in mysql_customers}

# Update order counts
for order in mongo_orders:
    custkey = str(order["O_CUSTKEY"])
    if custkey in order_count_dict:
        order_count_dict[custkey] += 1

# Calculate distribution
distribution = {}
for count in order_count_dict.values():
    distribution[count] = distribution.get(count, 0) + 1

# Write to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['Number of Orders', 'Number of Customers'])
    for orders, customers in sorted(distribution.items()):
        csvwriter.writerow([orders, customers])

# Close connections
mysql_conn.close()
mongo_client.close()
