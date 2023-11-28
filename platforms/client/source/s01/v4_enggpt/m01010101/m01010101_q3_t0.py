import pymysql
import pymongo
import csv
from datetime import datetime

# Function to connect to MySQL
def connect_mysql(host, user, password, db):
    return pymysql.connect(host=host, user=user, password=password, db=db)

# Function to connect to MongoDB
def connect_mongo(host, port, db):
    client = pymongo.MongoClient(host=host, port=port)
    return client[db]

mysql_conn = connect_mysql('mysql', 'root', 'my-secret-pw', 'tpch')
mongo_db = connect_mongo('mongodb', 27017, 'tpch')

# Query MySQL
mysql_cursor = mysql_conn.cursor()
mysql_query = """
SELECT O_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY, O_TOTALPRICE
FROM orders
WHERE O_ORDERDATE < '1995-03-15'
"""
mysql_cursor.execute(mysql_query)
orders_data = mysql_cursor.fetchall()

# Query MongoDB for customers in 'BUILDING' segment
building_customers = list(mongo_db.customer.find({"C_MKTSEGMENT": "BUILDING"}, {"C_CUSTKEY": 1}))

# Extract customer keys for SQL query
customer_keys = [customer['C_CUSTKEY'] for customer in building_customers]

# Query MongoDB for lineitems with L_SHIPDATE after '1995-03-15'
lineitems = mongo_db.lineitem.find({
    "L_ORDERKEY": {"$in": [row[0] for row in orders_data]},
    "L_SHIPDATE": {"$gt": datetime(1995, 3, 15)}
})

# Prepare lineitem data with applied discounts
lineitem_data = {}
for item in lineitems:
    extended_price = item["L_EXTENDEDPRICE"] * (1 - item["L_DISCOUNT"])
    if item["L_ORDERKEY"] not in lineitem_data:
        lineitem_data[item["L_ORDERKEY"]] = extended_price
    else:
        lineitem_data[item["L_ORDERKEY"]] += extended_price

# Combine data and write to CSV
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["O_ORDERKEY", "O_ORDERDATE", "O_SHIPPRIORITY", "REVENUE"])

    for order in orders_data:
        if order[0] in customer_keys and order[0] in lineitem_data:
            writer.writerow([order[0], order[1].strftime('%Y-%m-%d'), order[2], round(lineitem_data[order[0]], 2)])

# Clean up
mysql_cursor.close()
mysql_conn.close()
