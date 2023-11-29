import pymysql
import pymongo
import csv

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4'
)

# MongoDB connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Fetching customer IDs from MongoDB
customer_ids = mongodb_db.customer.distinct("C_CUSTKEY")

# Processing the query with MySQL
order_query = """
SELECT O_CUSTKEY, COUNT(*) as order_count
FROM orders
WHERE O_COMMENT NOT LIKE '%pending%deposits%'
GROUP BY O_CUSTKEY
HAVING O_CUSTKEY IN ({})
"""

# Formatting the customer_ids for SQL query
customer_ids_sql = ', '.join(str(id) for id in customer_ids)

# Executing the query
with mysql_conn.cursor() as cursor:
    cursor.execute(order_query.format(customer_ids_sql))
    order_results = cursor.fetchall()

# Count the number of orders per customer
order_counts = {}
for result in order_results:
    count = result[1]
    if count in order_counts:
        order_counts[count] += 1
    else:
        order_counts[count] = 1

# Close the MySQL connection
mysql_conn.close()

# Writing results to CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Number of Orders', 'Number of Customers'])
    for count, num_customers in order_counts.items():
        writer.writerow([count, num_customers])
