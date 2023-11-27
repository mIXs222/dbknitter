import pymysql
import pymongo
import csv

# Establish a connection to MySQL database
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Establish a connection to MongoDB database
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
mongodb_orders = mongodb_db['orders']

# Fetch large volume orders from MongoDB database
large_volume_orders = list(mongodb_orders.find({'O_TOTALPRICE': {'$gt': 300}}, {'_id': 0}))

# Get the order keys for large volume orders
order_keys = [order['O_ORDERKEY'] for order in large_volume_orders]

# Query MySQL database for customer details based on MongoDB order keys
mysql_cursor.execute(
    '''
    SELECT c.C_NAME, c.C_CUSTKEY, l.L_ORDERKEY, o.O_ORDERDATE, o.O_TOTALPRICE, SUM(l.L_QUANTITY) as total_quantity
    FROM lineitem l
    JOIN orders o ON l.L_ORDERKEY = o.O_ORDERKEY
    JOIN customer c ON o.O_CUSTKEY = c.C_CUSTKEY
    WHERE o.O_ORDERKEY IN (%s)
    GROUP BY l.L_ORDERKEY
    HAVING total_quantity > 300
    ''', [','.join(map(str, order_keys))]
)
result = mysql_cursor.fetchall()

# Write the query result to a CSV file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Customer Name', 'Customer Key', 'Order Key', 'Order Date', 'Total Price', 'Quantity'])
    for row in result:
        writer.writerow(row)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()
