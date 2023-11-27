import pymysql
import pymongo
import csv

# MySQL connection setup
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# MongoDB connection setup
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Fetching customers from MongoDB
customers = {cust['C_CUSTKEY']: cust['C_NAME'] for cust in mongodb.customer.find()}

# Query to fetch large quantity orders from MySQL
query = """
SELECT
    O_CUSTKEY,
    O_ORDERKEY,
    O_ORDERDATE,
    O_TOTALPRICE
FROM
    orders
WHERE
    O_ORDERKEY IN (
        SELECT L_ORDERKEY
        FROM lineitem
        GROUP BY L_ORDERKEY
        HAVING SUM(L_QUANTITY) > 300
    )
"""

# Execute the MySQL query
with mysql_connection.cursor() as cursor:
    cursor.execute(query)
    large_volume_orders = cursor.fetchall()

# Populate the final result
results = [
    [
        customers[row[0]],
        row[0],
        row[1],
        row[2].strftime('%Y-%m-%d'),
        str(row[3]),
        str(quantity)
    ]
    for row in large_volume_orders
    for quantity in mongodb.lineitem.find({'L_ORDERKEY': row[1]}, {'L_QUANTITY': 1, '_id': 0})
    if quantity and quantity['L_QUANTITY'] > 300
]

# Write results to CSV
with open('query_output.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['Customer Name', 'Customer Key', 'Order Key', 'Order Date', 'Total Price', 'Quantity'])
    writer.writerows(results)

# Close connections
mysql_connection.close()
mongo_client.close()
