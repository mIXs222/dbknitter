import pymysql
import pymongo
import csv

# Function to get data from MySQL
def get_mysql_data(conn_info, query):
    conn = pymysql.connect(host=conn_info['hostname'],
                           user=conn_info['username'],
                           password=conn_info['password'],
                           database=conn_info['database'])
    results = []
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            for row in cursor:
                results.append(row)
    finally:
        conn.close()
    return results

# Function to get data from MongoDB
def get_mongodb_data(conn_info, pipeline):
    client = pymongo.MongoClient(host=conn_info['hostname'], port=conn_info['port'])
    db = client[conn_info['database']]
    results = list(db.orders.aggregate(pipeline))
    client.close()
    return results

# MySQL connection information
mysql_conn_info = {
    'database': 'tpch',
    'username': 'root',
    'password': 'my-secret-pw',
    'hostname': 'mysql'
}

# MongoDB connection information
mongodb_conn_info = {
    'database': 'tpch',
    'hostname': 'mongodb',
    'port': 27017
}

# Query for MySQL
mysql_query = """
SELECT C_CUSTKEY, C_NAME FROM customer
"""

# Pipeline for MongoDB
mongodb_pipeline = [
    {
        '$lookup': {
            'from': 'lineitem',
            'localField': 'O_ORDERKEY',
            'foreignField': 'L_ORDERKEY',
            'as': 'items'
        }
    },
    {
        '$unwind': '$items'
    },
    {
        '$group': {
            '_id': {
                'O_ORDERKEY': '$O_ORDERKEY',
                'O_CUSTKEY': '$O_CUSTKEY',
                'O_TOTALPRICE': '$O_TOTALPRICE',
                'O_ORDERDATE': '$O_ORDERDATE'
            },
            'quantity_sum': {
                '$sum': '$items.L_QUANTITY'
            }
        }
    },
    {
        '$match': {
            'quantity_sum': {'$gt': 300}
        }
    },
    {
        '$project': {
            '_id': 0,
            'O_ORDERKEY': '$_id.O_ORDERKEY',
            'O_CUSTKEY': '$_id.O_CUSTKEY',
            'O_TOTALPRICE': '$_id.O_TOTALPRICE',
            'O_ORDERDATE': '$_id.O_ORDERDATE',
            'quantity_sum': 1
        }
    },
    {
        '$sort': {
            'O_TOTALPRICE': -1,
            'O_ORDERDATE': 1
        }
    }
]

# Collect data
mysql_customers = get_mysql_data(mysql_conn_info, mysql_query)
mongodb_orders = get_mongodb_data(mongodb_conn_info, mongodb_pipeline)

# Map customers to orders
customer_orders = []
for customer in mysql_customers:
    for order in mongodb_orders:
        if customer[0] == order['O_CUSTKEY']:
            customer_orders.append((customer[1], customer[0], order['O_ORDERKEY'], order['O_ORDERDATE'], order['O_TOTALPRICE'], order['quantity_sum']))

# Write to CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'quantity_sum'])
    for record in customer_orders:
        writer.writerow(record)
