import pymysql
import pymongo
import csv

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_customers = mongo_db['customer']
mongo_lineitems = mongo_db['lineitem']

# Get order keys with total quantity > 300 from mongodb lineitem table
aggregation_pipeline = [
    {
        '$group': {
            '_id': "$L_ORDERKEY",
            'total_quantity': {'$sum': "$L_QUANTITY"}
        }
    },
    {'$match': {'total_quantity': {'$gt': 300}}}
]
order_keys_with_total_quantity_gt_300 = list(mongo_lineitems.aggregate(aggregation_pipeline))
order_keys = [doc['_id'] for doc in order_keys_with_total_quantity_gt_300]

# Retrieve matching orders from MySQL
sql_orders_query = 'SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE FROM orders WHERE O_ORDERKEY IN (%s)'
format_strings = ','.join(['%s'] * len(order_keys))
mysql_cursor.execute(sql_orders_query % format_strings, tuple(order_keys))

orders_data = {}
for order in mysql_cursor.fetchall():
    orders_data[order[0]] = {
        'O_CUSTKEY': order[1],
        'O_ORDERSTATUS': order[2],
        'O_TOTALPRICE': order[3],
        'O_ORDERDATE': order[4]
    }

# Retrieve customer information from MongoDB
customer_data = {}
for customer in mongo_customers.find({'C_CUSTKEY': {'$in': [orders_data[order_key]['O_CUSTKEY'] for order_key in orders_data]}}):
    customer_data[customer['C_CUSTKEY']] = customer['C_NAME']

# Write results to file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'Total_Quantity'])
    
    # Retrieve line item data for orders and write relevant information
    for lineitem in mongo_lineitems.find({'L_ORDERKEY': {'$in': order_keys}}):
        if lineitem['L_ORDERKEY'] in orders_data and orders_data[lineitem['L_ORDERKEY']]['O_CUSTKEY'] in customer_data:
            row = [
                customer_data[orders_data[lineitem['L_ORDERKEY']]['O_CUSTKEY']],
                orders_data[lineitem['L_ORDERKEY']]['O_CUSTKEY'],
                lineitem['L_ORDERKEY'],
                orders_data[lineitem['L_ORDERKEY']]['O_ORDERDATE'],
                orders_data[lineitem['L_ORDERKEY']]['O_TOTALPRICE'],
                lineitem['L_QUANTITY'] # Assuming we want individual lineitem quantities
            ]
            writer.writerow(row)

# Close connections
mysql_conn.close()
mongo_client.close()
