import csv
import mysql.connector
import pymongo

# Establish connection to MySQL
conn_mysql = mysql.connector.connect(user='root', password='my-secret-pw', host='mysql', database='tpch')

# Establish connection to MongoDB
client = pymongo.MongoClient('mongodb', 27017)
db_mongodb = client['tpch']

customers_mongo = db_mongodb.customer.find({})
orders_mysql = conn_mysql.cursor()

output = []

for customer in customers_mongo:
    if str(customer['C_PHONE'][:2]) in ['20', '40', '22', '30', '39', '42', '21'] and customer['C_ACCTBAL'] > 0:
        order_exits = False
        orders_mysql.execute(f'SELECT * FROM orders WHERE O_CUSTKEY = {customer["C_CUSTKEY"]}')
        orders = orders_mysql.fetchall()
        if len(orders) > 0:
            order_exits = True

        if not order_exits:
            output.append([
                customer['C_PHONE'][:2],
                customer['C_ACCTBAL']
            ])

# Writing to file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(output)

# Close the connections
conn_mysql.close()
client.close()
