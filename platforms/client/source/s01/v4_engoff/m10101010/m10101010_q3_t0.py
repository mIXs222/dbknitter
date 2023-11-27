import pymysql
import pymongo
import csv
from datetime import datetime

# Constants
MYSQL_CONN_INFO = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch',
}
MONGODB_CONN_INFO = {
    'host': 'mongodb',
    'port': 27017,
}
TARGET_DATE = datetime(1995, 3, 15)
MARKET_SEGMENT = 'BUILDING'

# Function to perform MySQL queries
def query_mysql(sql):
    conn = pymysql.connect(**MYSQL_CONN_INFO)
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()
    finally:
        conn.close()

# Function to perform MongoDB queries
def query_mongodb(filter):
    client = pymongo.MongoClient(MONGODB_CONN_INFO['host'], MONGODB_CONN_INFO['port'])
    db = client.tpch
    return list(db.orders.find(filter))

# Getting customer keys for the market segment BUILDING
mysql_query = """
SELECT C_CUSTKEY
FROM customer
WHERE C_MKTSEGMENT = %s
"""
customer_keys_tuples = query_mysql(mysql_query, (MARKET_SEGMENT,))
customer_keys = [key[0] for key in customer_keys_tuples]

# Getting order keys with a LINSTATUS of 'O' for non-shipped items
order_details = query_mongodb({
    'O_ORDERDATE': {'$lt': TARGET_DATE},
    'O_CUSTKEY': {'$in': customer_keys},
    'O_ORDERSTATUS': 'O'
})

# Building order priority, and revenue list
order_to_revenue = []
for order in order_details:
    order_priority = order['O_SHIPPRIORITY']
    order_key = order['O_ORDERKEY']

    mysql_query = """
    SELECT SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS revenue
    FROM lineitem
    WHERE L_ORDERKEY = %s
    GROUP BY L_ORDERKEY
    """
    revenue = query_mysql(mysql_query, (order_key,))

    if revenue:
        order_to_revenue.append((order_priority, revenue[0][0], order_key))

# Sort orders by revenue in descending order
order_to_revenue = sorted(order_to_revenue, key=lambda x: x[1], reverse=True)

# Output to file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['O_SHIPPRIORITY', 'revenue', 'O_ORDERKEY'])
    for row in order_to_revenue:
        writer.writerow(row)
