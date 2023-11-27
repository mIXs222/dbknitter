# query_combined.py
import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_connection.cursor()

# Fetch orders data from MySQL where O_COMMENT does not contain 'pending%deposits'
mysql_cursor.execute("SELECT O_ORDERKEY, O_CUSTKEY FROM orders WHERE O_COMMENT NOT LIKE '%pending%deposits%'")
orders_data = mysql_cursor.fetchall()

# Map customer key to order counts
custkey_order_count = {}
for order in orders_data:
    custkey_order_count[order[1]] = custkey_order_count.get(order[1], 0) + 1

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Get customer data from MongoDB
customer_data = mongodb_db.customer.find({}, {'_id': 0, 'C_CUSTKEY': 1})

# Calculate C_COUNT for each customer
custkey_to_ccount = {}
for customer in customer_data:
    cust_key = customer['C_CUSTKEY']
    c_count = custkey_order_count.get(cust_key, 0)
    custkey_to_ccount[cust_key] = c_count

# Close MySQL connection
mysql_cursor.close()
mysql_connection.close()

# Group by C_COUNT to calculate CUSTDIST
ccount_to_custdist = {}
for c_count in custkey_to_ccount.values():
    ccount_to_custdist[c_count] = ccount_to_custdist.get(c_count, 0) + 1

# Write the query output to CSV
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['C_COUNT', 'CUSTDIST'])
    for c_count, custdist in sorted(ccount_to_custdist.items(), key=lambda item: (-item[1], -item[0])):
        writer.writerow([c_count, custdist])

# Close MongoDB connection
mongodb_client.close()
