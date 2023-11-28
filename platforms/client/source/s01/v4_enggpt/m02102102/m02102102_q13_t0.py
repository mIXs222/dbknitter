import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_customers = mongo_db['customer']

# Perform left join operation manually because of cross-platform databases
# Fetching customer IDs and names from MongoDB
customers = list(mongo_customers.find({}, {'C_CUSTKEY': 1, '_id': 0}))

# Fetching filtered orders from MySQL and counting per customer
mysql_cursor.execute("""
SELECT O_CUSTKEY, COUNT(*) as C_ORDER_COUNT FROM orders
WHERE O_COMMENT NOT LIKE '%pending%' AND O_COMMENT NOT LIKE '%deposits%'
GROUP BY O_CUSTKEY
""")
orders_count_per_customer = {row[0]: row[1] for row in mysql_cursor.fetchall()}

# Combining data
customer_order_counts = {customer['C_CUSTKEY']: orders_count_per_customer.get(customer['C_CUSTKEY'], 0) for customer in customers}

# Distributing customers by order count
distribution = {}
for count in customer_order_counts.values():
    distribution[count] = distribution.get(count, 0) + 1

# Formatting final output
output_rows = [{'C_COUNT': count, 'CUSTDIST': cust_dist} for count, cust_dist in distribution.items()]
# Sort by CUSTDIST descending, then by C_COUNT descending
output_rows.sort(key=lambda x: (-x['CUSTDIST'], -x['C_COUNT']))

# Write to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['C_COUNT', 'CUSTDIST']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in output_rows:
        writer.writerow(row)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
