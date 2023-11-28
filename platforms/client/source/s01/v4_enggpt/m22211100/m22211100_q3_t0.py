import pymysql
import pymongo
import csv
from datetime import datetime

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql', 
    user='root', 
    password='my-secret-pw', 
    database='tpch'
)

# MongoDB connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Fetch customers from the 'BUILDING' market segment
building_customers = list(mongodb_db.customer.find(
    {"C_MKTSEGMENT": "BUILDING"}, 
    {"_id": 0, "C_CUSTKEY": 1}
))

# Extract customer keys
cust_keys = [customer['C_CUSTKEY'] for customer in building_customers]

# Prepare MySQL queries
orders_query = """
SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERDATE, O_SHIPPRIORITY
FROM orders
WHERE O_CUSTKEY IN %s AND O_ORDERDATE < '1995-03-15'
"""

lineitem_query = """
SELECT L_ORDERKEY,
       SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS revenue
FROM lineitem
WHERE L_SHIPDATE > '1995-03-15' AND L_ORDERKEY IN %s
GROUP BY L_ORDERKEY
"""

# Execute MySQL queries
with mysql_conn.cursor() as cursor:
    cursor.execute(orders_query, (cust_keys,))
    orders = cursor.fetchall()

    # Filter order keys
    order_keys = [order[0] for order in orders]

    cursor.execute(lineitem_query, (order_keys,))
    lineitems = {lineitem[0]: lineitem[1] for lineitem in cursor.fetchall()}

# Combine data and write to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY', 'REVENUE'])
    
    for order in orders:
        order_key, _, order_date, ship_priority = order
        revenue = lineitems.get(order_key)
        if revenue:
            writer.writerow([
                order_key, 
                datetime.strftime(order_date, '%Y-%m-%d'), 
                ship_priority, 
                revenue
            ])

# Close connections
mysql_conn.close()
mongodb_client.close()
