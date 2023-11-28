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

# Prepare data structures
customer_orders = {}

# Get customer data from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT C_CUSTKEY, C_NAME FROM customer")
    for row in cursor.fetchall():
        # Initialize the customer in the dictionary
        customer_orders[row[0]] = {
            'C_NAME': row[1],
            'C_COUNT': 0,
            'CUSTDIST': 0
        }

# Get orders data from MongoDB
orders_collection = mongodb['orders']
orders_data = orders_collection.find({
    '$nor': [
        {'O_COMMENT': {'$regex': 'pending'}},
        {'O_COMMENT': {'$regex': 'deposits'}}
    ]
})

# Process orders data
for order in orders_data:
    cust_key = order['O_CUSTKEY']
    if cust_key in customer_orders:
        customer_orders[cust_key]['C_COUNT'] += 1

# Calculate CUSTDIST
for cust_key, values in customer_orders.items():
    count = values['C_COUNT']
    for key, vals in customer_orders.items():
        if vals['C_COUNT'] == count:
            customer_orders[key]['CUSTDIST'] += 1

# Prepare data for CSV output
output_data = []
for cust_key, vals in customer_orders.items():
    output_data.append((vals['C_COUNT'], vals['CUSTDIST'], vals['C_NAME']))

# Sort results as per the query requirements
output_data.sort(key=lambda x: (-x[1], -x[0]))

# Write output to CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['C_COUNT', 'CUSTDIST', 'C_NAME'])
    for row in output_data:
        writer.writerow(row)

# Close connections
mysql_conn.close()
mongo_client.close()
