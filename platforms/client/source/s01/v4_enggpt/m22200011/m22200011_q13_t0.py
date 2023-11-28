import pymysql
import pymongo
import csv

# Connecting to MySQL
mysql_conn = pymysql.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    database="tpch"
)
mysql_cursor = mysql_conn.cursor()

# Connecting to MongoDB
mongo_client = pymongo.MongoClient(host="mongodb", port=27017)
mongodb = mongo_client["tpch"]
orders_collection = mongodb["orders"]

# MySQL query for customer data
mysql_query = "SELECT C_CUSTKEY, C_NAME FROM customer"
mysql_cursor.execute(mysql_query)
customers = {row[0]: row[1] for row in mysql_cursor.fetchall()}

# Getting orders data from MongoDB
orders_query = {'O_COMMENT': {'$not': {'$regex': 'pending|deposits'}}}
orders_data = orders_collection.find(orders_query, {'O_CUSTKEY': 1})

# Count customer orders
order_counts = {}
for order in orders_data:
    custkey = order['O_CUSTKEY']
    if custkey not in order_counts:
        order_counts[custkey] = 0
    order_counts[custkey] += 1

# Combine results
combined_data = {}
for custkey, _ in customers.items():
    # Initialize dictionary with 0 count if customer has no orders meeting conditions
    combined_data[custkey] = order_counts.get(custkey, 0)

# Group by counts and get distribution
distribution = {}
for count in combined_data.values():
    if count not in distribution:
        distribution[count] = 0
    distribution[count] += 1

# Write results to CSV
with open('query_output.csv', mode='w') as file:
    writer = csv.writer(file)
    writer.writerow(['C_COUNT', 'CUSTDIST'])

    for count, numb in sorted(distribution.items(), key=lambda item: (-item[1], -item[0])):
        writer.writerow([count, numb])

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
