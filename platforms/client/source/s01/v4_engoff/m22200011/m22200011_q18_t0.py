# large_volume_customer_query.py
import pymysql
import pymongo
import csv

# Connect to the MySQL database
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to the MongoDB database
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
orders_collection = mongo_db.orders
lineitem_collection = mongo_db.lineitem

# Fetch customers from MySQL
mysql_cursor.execute("SELECT C_CUSTKEY, C_NAME FROM customer")
customers = {cust_key: name for cust_key, name in mysql_cursor.fetchall()}

# Fetch large volume orders from MongoDB
large_orders = orders_collection.aggregate([
    {"$lookup": {
        "from": "lineitem",
        "localField": "O_ORDERKEY",
        "foreignField": "L_ORDERKEY",
        "as": "lineitems"
    }},
    {"$unwind": "$lineitems"},
    {"$group": {
        "_id": {
            "O_ORDERKEY": "$O_ORDERKEY",
            "O_CUSTKEY": "$O_CUSTKEY",
            "O_ORDERDATE": "$O_ORDERDATE",
            "O_TOTALPRICE": "$O_TOTALPRICE"
        },
        "total_quantity": {"$sum": "$lineitems.L_QUANTITY"}
    }},
    {"$match": {"total_quantity": {"$gt": 300}}},
    {"$project": {
        "O_ORDERKEY": "$_id.O_ORDERKEY",
        "O_CUSTKEY": "$_id.O_CUSTKEY",
        "O_ORDERDATE": "$_id.O_ORDERDATE",
        "O_TOTALPRICE": "$_id.O_TOTALPRICE",
        "total_quantity": 1,
        "_id": 0
    }}
])

# Merge results from both databases and write to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'total_quantity']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for order in large_orders:
        if order['O_CUSTKEY'] in customers:
            writer.writerow({
                'C_NAME': customers[order['O_CUSTKEY']],
                'C_CUSTKEY': order['O_CUSTKEY'],
                'O_ORDERKEY': order['O_ORDERKEY'],
                'O_ORDERDATE': order['O_ORDERDATE'],
                'O_TOTALPRICE': order['O_TOTALPRICE'],
                'total_quantity': order['total_quantity']
            })

# Close connections
mysql_conn.close()
mongo_client.close()
