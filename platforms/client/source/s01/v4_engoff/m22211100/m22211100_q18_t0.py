import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   database='tpch',
                                   charset='utf8mb4',
                                   cursorclass=pymysql.cursors.Cursor)

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client['tpch']
customer_collection = mongo_db['customer']

# Query to get large orders from mysql
mysql_query = """
SELECT o.O_CUSTKEY, o.O_ORDERKEY, o.O_ORDERDATE, o.O_TOTALPRICE, SUM(l.L_QUANTITY) as total_quantity
FROM orders o
JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
GROUP BY o.O_ORDERKEY
HAVING SUM(l.L_QUANTITY) > 300
"""

# Fetch large orders from mysql
mysql_cursor = mysql_connection.cursor()
mysql_cursor.execute(mysql_query)
large_orders = mysql_cursor.fetchall()
mysql_cursor.close()

# Fetch customer data from mongodb
customers_data = list(customer_collection.find({}, {'_id': 0}))

# Combine the data from MySQL and MongoDB
final_results = []
for customer in customers_data:
    for order in large_orders:
        if customer['C_CUSTKEY'] == order[0]:
            final_results.append({
                'C_NAME': customer['C_NAME'],
                'C_CUSTKEY': customer['C_CUSTKEY'],
                'O_ORDERKEY': order[1],
                'O_ORDERDATE': order[2],
                'O_TOTALPRICE': order[3],
                'TOTAL_QUANTITY': order[4]
            })

# Write results to CSV file
with open('query_output.csv', mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    # Write header
    writer.writerow(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'TOTAL_QUANTITY'])
    # Write data
    for result in final_results:
        writer.writerow([result['C_NAME'], result['C_CUSTKEY'], result['O_ORDERKEY'], result['O_ORDERDATE'],
                         result['O_TOTALPRICE'], result['TOTAL_QUANTITY']])

# Close the connections
mysql_connection.close()
mongo_client.close()
