import pymysql
import pymongo
import csv

# Connect to MySQL database
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Fetch data from MySQL
mysql_query = """
SELECT o.O_ORDERKEY, o.O_CUSTKEY, o.O_ORDERDATE, o.O_TOTALPRICE, SUM(l.L_QUANTITY) as total_quantity
FROM orders o JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
GROUP BY o.O_ORDERKEY
HAVING SUM(l.L_QUANTITY) > 300;
"""
mysql_cursor.execute(mysql_query)
orders_lineitems = mysql_cursor.fetchall()

# Extract the order keys for matching with customers
order_keys = [order[0] for order in orders_lineitems]

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client.tpch
customers_collection = mongo_db.customer

# Fetch data from MongoDB
customers = list(customers_collection.find({'C_CUSTKEY': {'$in': [order[1] for order in orders_lineitems]}}))

# Integrate and prepare the final output
output = []
for order in orders_lineitems:
    for customer in customers:
        if customer['C_CUSTKEY'] == order[1]:
            output_record = {
                'C_NAME': customer['C_NAME'],
                'C_CUSTKEY': customer['C_CUSTKEY'],
                'O_ORDERKEY': order[0],
                'O_ORDERDATE': order[2],
                'O_TOTALPRICE': order[3],
                'SUM(L_QUANTITY)': order[4],
            }
            output.append(output_record)

# Sort by total price descending and then by order date
output.sort(key=lambda x: (-x['O_TOTALPRICE'], x['O_ORDERDATE']))

# Write output to CSV
with open('query_output.csv', mode='w') as csv_file:
    fieldnames = ['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'SUM(L_QUANTITY)']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    writer.writeheader()
    for record in output:
        writer.writerow(record)

# Close the connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
