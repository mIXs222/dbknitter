import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch')
mysql_cursor = mysql_connection.cursor()

# Get all lineitems with SUM(L_QUANTITY) > 300
mysql_cursor.execute("""
    SELECT L_ORDERKEY
    FROM lineitem
    GROUP BY L_ORDERKEY
    HAVING SUM(L_QUANTITY) > 300
""")
order_keys = [row[0] for row in mysql_cursor.fetchall()]

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
orders_collection = mongodb_db['orders']

# Get all orders that match the given lineitems and their related customer keys
orders = orders_collection.find({'O_ORDERKEY': {'$in': order_keys}}, {'_id': 0, 'O_CUSTKEY': 1, 'O_ORDERKEY': 1, 'O_ORDERDATE': 1, 'O_TOTALPRICE': 1})
order_info = list(orders)

# Extract customer keys for customer collection query
customer_keys = [order['O_CUSTKEY'] for order in order_info]

# Get customers that match the customer keys
mysql_cursor.execute("SELECT C_NAME, C_CUSTKEY FROM customer WHERE C_CUSTKEY IN (%s)" % ','.join(['%s'] * len(customer_keys)), customer_keys)
customers = {C_CUSTKEY: C_NAME for C_NAME, C_CUSTKEY in mysql_cursor.fetchall()}

# Aggregate final data
final_data = []
for order in order_info:
    if order['O_CUSTKEY'] in customers:
        mysql_cursor.execute("""
            SELECT SUM(L_QUANTITY)
            FROM lineitem
            WHERE L_ORDERKEY = %s
            GROUP BY L_ORDERKEY
        """, (order['O_ORDERKEY'],))
        total_quantity = mysql_cursor.fetchone()[0]
        final_data.append([
            customers[order['O_CUSTKEY']],
            order['O_CUSTKEY'],
            order['O_ORDERKEY'],
            order['O_ORDERDATE'],
            order['O_TOTALPRICE'],
            total_quantity
        ])

# Sort the final data
final_data.sort(key=lambda x: (-x[4], x[3]))  # Sort by O_TOTALPRICE descending, then by O_ORDERDATE

# Write to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'SUM_L_QUANTITY'])
    for row in final_data:
        writer.writerow(row)

# Close connections
mysql_cursor.close()
mysql_connection.close()
mongodb_client.close()
