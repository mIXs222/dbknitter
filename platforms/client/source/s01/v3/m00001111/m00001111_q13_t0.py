import csv
import pymongo
from pymongo import MongoClient
from mysql.connector import connect, Error

# MongoDB Connection
client = MongoClient('mongodb', 27017)
db = client['tpch']
customer = db['customer']
orders = db['orders']

# Mysql Connection
try:
    with connect(
        host="localhost",
        user=input("Enter username: "),
        password=input("Enter password: "),
    ) as connection:
        print(connection)
except Error as e:
    print(e)

# Fetch data from MongoDB    
customer_data = customer.find()
orders_data = orders.find()

# Create data tables
customer_table = {c['C_CUSTKEY']: c for c in customer_data}
orders_table = {o['O_CUSTKEY']: o for o in orders_data if 'pending deposits' not in o['O_COMMENT']}

# Merge data
merged_data = []
for c_key, c_val in customer_table.items():
    if c_key in orders_table:
        merged_data.append({**c_val, **orders_table[o_key]})

# Generate output
output = []
for item in merged_data:
    output.append([item['C_CUSTKEY'], len([o for o in orders_table if o == item['C_CUSTKEY']])])

# Write to csv
with open('query_output.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerows(output)
