import pymysql
import pymongo
import csv
from datetime import datetime

# MySQL connection setup
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
)

# MongoDB connection setup
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Prepare the data structures for the result
nation_revenue = {}

# Query and process MySQL data (Nation and Region tables)
with mysql_connection.cursor() as cursor:
    # SQL to join nation and region and filter on 'ASIA'
    sql = """
    SELECT n.N_NAME, n.N_NATIONKEY
    FROM nation n
    JOIN region r ON n.N_REGIONKEY = r.R_REGIONKEY
    WHERE r.R_NAME = 'ASIA'
    """
    cursor.execute(sql)
    for nation_name, nation_key in cursor:
        nation_revenue[nation_key] = {'nation_name': nation_name, 'revenue': 0}

# Processing MongoDB data
# Getting customers from 'ASIA'
customers = list(mongodb.customer.find(
    {'C_NATIONKEY': {'$in': list(nation_revenue.keys())}},
    {'C_CUSTKEY': 1, 'C_NATIONKEY': 1}
))
# Maps customer key to nation key
customer_to_nation = {customer['C_CUSTKEY']: customer['C_NATIONKEY'] for customer in customers}

# Relevant orders based on timeframe
start_date = datetime(1990, 1, 1)
end_date = datetime(1994, 12, 31)
orders = list(mongodb.orders.find(
    {'O_CUSTKEY': {'$in': list(customer_to_nation.keys())}, 'O_ORDERDATE': {'$gte': start_date, '$lte': end_date}},
    {'O_ORDERKEY': 1, 'O_CUSTKEY': 1}
))
# Maps order key to customer key
order_to_customer = {order['O_ORDERKEY']: order['O_CUSTKEY'] for order in orders}

# Line item records processing
line_items = mongodb.lineitem.find(
    {'L_ORDERKEY': {'$in': list(order_to_customer.keys())}},
    {'L_ORDERKEY': 1, 'L_EXTENDEDPRICE': 1, 'L_DISCOUNT': 1}
)
for line_item in line_items:
    order_key = line_item['L_ORDERKEY']
    # Calculate revenue
    revenue = line_item['L_EXTENDEDPRICE'] * (1 - line_item['L_DISCOUNT'])
    customer_key = order_to_customer[order_key]
    nation_key = customer_to_nation[customer_key]
    nation_revenue[nation_key]['revenue'] += revenue

# Sort results based on revenue
sorted_nations = sorted(nation_revenue.values(), key=lambda x: x['revenue'], reverse=True)

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['nation_name', 'revenue']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for record in sorted_nations:
        writer.writerow(record)

# Close the connections
mysql_connection.close()
mongo_client.close()
