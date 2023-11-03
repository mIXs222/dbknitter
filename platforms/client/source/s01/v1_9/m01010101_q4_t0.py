import mysql.connector
from pymongo import MongoClient
from pandas import DataFrame

# Connect to the MySQL and MongoDB databases
mysql_conn = mysql.connector.connect(user='root', password='my-secret-pw', host='mysql', database='tpch')
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Perform the query on the MySQL part of the data
order_cursor = mysql_conn.cursor()
order_cursor.execute("""
    SELECT O_ORDERPRIORITY, O_ORDERKEY
    FROM ORDERS
    WHERE O_ORDERDATE >= '1993-07-01'
    AND O_ORDERDATE < '1993-10-01'
""")

# Create a dictionary of {'order_key': 'order_priority'} to store the MySQL data
order_dict = {key: priority for priority, key in order_cursor}

# Perform the query on the MongoDB part of the data and count orders
order_count = dict()

for lineitem in mongo_db.lineitem.find({'L_COMMITDATE': {'$lt': 'L_RECEIPTDATE'}}):
    order_key = lineitem['L_ORDERKEY']
    if order_key in order_dict:
        priority = order_dict[order_key]
        if priority in order_count:
            order_count[priority] += 1
        else:
            order_count[priority] = 1

# Convert the result into dataframe and write to CSV
result = DataFrame.from_dict(order_count, orient='index', columns=['ORDER_COUNT'])
result.index.name = 'O_ORDERPRIORITY'
result.sort_values(by='O_ORDERPRIORITY', inplace=True)
result.to_csv('query_output.csv')
