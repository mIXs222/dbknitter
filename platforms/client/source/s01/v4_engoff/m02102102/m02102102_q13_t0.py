import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.Cursor
)

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
customer_collection = mongodb_db['customer']

# Fetch customers from MongoDB
mongo_customers = {doc['C_CUSTKEY']: doc for doc in customer_collection.find()}

# Fetch non-pending and non-deposit orders from MySQL
try:
    with mysql_conn.cursor() as cursor:
        cursor.execute("""
            SELECT O_CUSTKEY, COUNT(*) as order_count
            FROM orders
            WHERE O_ORDERSTATUS NOT IN ('P', 'D')
            AND O_COMMENT NOT LIKE '%pending%'
            AND O_COMMENT NOT LIKE '%deposits%'
            GROUP BY O_CUSTKEY;
        """)
        mysql_orders = cursor.fetchall()
finally:
    mysql_conn.close()

order_count_per_customer = {}
for row in mysql_orders:
    custkey, order_count = row
    order_count_per_customer[custkey] = order_count

# Combine results
final_distribution = {}
for custkey in mongo_customers:
    order_count = order_count_per_customer.get(custkey, 0)
    if order_count not in final_distribution:
        final_distribution[order_count] = 0
    final_distribution[order_count] += 1

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Number_of_Orders', 'Number_of_Customers'])
    for order_count, num_customers in sorted(final_distribution.items()):
        writer.writerow([order_count, num_customers])
