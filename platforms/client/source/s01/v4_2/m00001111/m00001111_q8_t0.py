import csv
import pymongo
import pymysql
from datetime import datetime

# Database configurations
mongo_config = {
    'dbName': 'tpch',
    'host': 'mongodb',
    'port': 27017,
}

mysql_config = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch',
}

# MongoDB connection
mongo_client = pymongo.MongoClient(mongo_config['host'], mongo_config['port'])
mongo_db = mongo_client[mongo_config['dbName']]
orders_collection = mongo_db['orders']
lineitem_collection = mongo_db['lineitem']
customer_collection = mongo_db['customer']

# MySQL connection
mysql_conn = pymysql.connect(host=mysql_config['host'],
        user=mysql_config['user'], password=mysql_config['password'],
        database=mysql_config['database'])
cursor = mysql_conn.cursor()
cursor.execute("SELECT N_NATIONKEY, N_NAME, R_REGIONKEY FROM nation JOIN region ON N_REGIONKEY = R_REGIONKEY WHERE N_NAME = 'INDIA' AND R_NAME='ASIA'")
india_info = cursor.fetchone()

# Query orders from MongoDB
orders = orders_collection.find({
    "O_ORDERDATE": {"$gte": datetime.strptime("1995-01-01", "%Y-%m-%d"), "$lte": datetime.strptime("1996-12-31", "%Y-%m-%d")},
    "O_CUSTKEY": india_info[0]
})
orders_dict = {order['O_ORDERKEY'] : order for order in orders}

# Query lineitems from MongoDB
lineitems = lineitem_collection.find({})
lineitems_dict = {lineitem['L_ORDERKEY'] : lineitem for lineitem in lineitems}

# Query customers from MongoDB
customers = customer_collection.find({})
customers_dict = {customer['C_CUSTKEY'] : customer for customer in customers}

# Write results into CSV
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["O_YEAR", "MKT_SHARE"])
    for order_key, order in orders_dict.items():
        lineitem = lineitems_dict.get(order_key)
        customer = customers_dict.get(order['O_CUSTKEY'])
        if lineitem and customer and lineitem['L_PARTKEY'] == 'SMALL PLATED COPPER':
            writer.writerow([datetime.strftime(order['O_ORDERDATE'], '%Y'), lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])])
