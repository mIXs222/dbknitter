# analysis.py

import pymysql
import pymongo
import csv

# Define connection parameters for MySQL
mysql_conn_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch'
}

# Establish connection to MySQL
mysql_conn = pymysql.connect(**mysql_conn_params)
mysql_cursor = mysql_conn.cursor()

# Perform subquery in MySQL to get order keys with total quantity over 300
mysql_cursor.execute('''
    SELECT L_ORDERKEY, SUM(L_QUANTITY) as total_quantity
    FROM lineitem
    GROUP BY L_ORDERKEY
    HAVING total_quantity > 300;
''')

# Fetch the results
order_keys_with_high_qty = mysql_cursor.fetchall()

# Convert results into a list of order keys
high_qty_order_keys = [row[0] for row in order_keys_with_high_qty]

# Close the MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Define connection parameters for MongoDB
mongodb_conn_params = {
    'host': 'mongodb',
    'port': 27017
}

# Establish connection to MongoDB
mongodb_conn = pymongo.MongoClient(**mongodb_conn_params)
mongodb_db = mongodb_conn['tpch']
orders_collection = mongodb_db['orders']

# Perform query in MongoDB to get orders matching the high quantity order keys
orders_with_high_qty = list(orders_collection.find({
    'O_ORDERKEY': {'$in': high_qty_order_keys}
}, {
    'O_ORDERKEY': 1, 'O_CUSTKEY': 1, 'O_ORDERDATE': 1, 'O_TOTALPRICE': 1
}))

# Convert MongoDB results to a dictionary for easy lookup
orders_dict = {order['O_ORDERKEY']: order for order in orders_with_high_qty}

# Establish connection to MySQL to fetch customer details
mysql_conn = pymysql.connect(**mysql_conn_params)
mysql_cursor = mysql_conn.cursor()

# Write results to CSV
with open('query_output.csv', mode='w') as file:
    writer = csv.writer(file)
    # Write Header
    writer.writerow(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'TOTAL_QUANTITY'])

    # Iterate through order keys with high quantities
    for order_key in high_qty_order_keys:
        # Ensure the order exists in the orders_dict
        if order_key not in orders_dict:
            continue

        order = orders_dict[order_key]

        # Fetch customer information matching the order's customer key
        mysql_cursor.execute('''
            SELECT C_NAME, C_CUSTKEY
            FROM customer
            WHERE C_CUSTKEY = %s
        ''', (order['O_CUSTKEY'],))

        # Fetch customer data from MySQL
        customer_data = mysql_cursor.fetchone()

        # Write a row with the combined data
        writer.writerow([
            customer_data[0],  # C_NAME
            customer_data[1],  # C_CUSTKEY
            order['O_ORDERKEY'],  # O_ORDERKEY
            order['O_ORDERDATE'],  # O_ORDERDATE
            order['O_TOTALPRICE'],  # O_TOTALPRICE
            sum([li[1] for li in high_qty_order_keys if li[0] == order_key])  # TOTAL_QUANTITY
        ])

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Close MongoDB connection
mongodb_conn.close()
