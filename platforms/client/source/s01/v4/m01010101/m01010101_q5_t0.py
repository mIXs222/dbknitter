# query_execution.py
import pymongo
import pymysql
import csv
from decimal import Decimal

# Connect to the MySQL database
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to the MongoDB database
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Get data from the nation table in MySQL
nation_query = 'SELECT N_NATIONKEY, N_NAME, N_REGIONKEY FROM nation'
mysql_cursor.execute(nation_query)
nation_data = {row[0]: row[1:] for row in mysql_cursor.fetchall()}

# Get data from the region table in MongoDB
regions = list(mongo_db.region.find({'R_NAME': 'ASIA'}, {'_id': 0, 'R_REGIONKEY': 1}))

# Get data from the orders table in MySQL
orders_query = 'SELECT O_ORDERKEY, O_CUSTKEY FROM orders WHERE O_ORDERDATE >= "1990-01-01" AND O_ORDERDATE < "1995-01-01"'
mysql_cursor.execute(orders_query)
orders_data = {row[0]: row[1] for row in mysql_cursor.fetchall()}

# Get data from lineitem in MongoDB
lineitems = list(mongo_db.lineitem.find(
    {
        'L_ORDERKEY': {'$in': list(orders_data.keys())}
    },
    {'_id': 0, 'L_ORDERKEY': 1, 'L_EXTENDEDPRICE': 1, 'L_DISCOUNT': 1, 'L_SUPPKEY': 1}
))

# Get data from the supplier table in MongoDB
suppliers = list(mongo_db.supplier.find({}, {'_id': 0, 'S_SUPPKEY': 1, 'S_NATIONKEY': 1}))

# Get data from the customer table in MongoDB
customers = list(mongo_db.customer.find(
    {
        'C_CUSTKEY': {'$in': list(orders_data.values())}
    },
    {'_id': 0, 'C_CUSTKEY': 1, 'C_NATIONKEY': 1}
))

# Close the MySQL cursor and connection
mysql_cursor.close()
mysql_conn.close()

# Prepare data for processing
supplier_data = {s['S_SUPPKEY']: s['S_NATIONKEY'] for s in suppliers}
customer_data = {c['C_CUSTKEY']: c['C_NATIONKEY'] for c in customers}
region_data = [r['R_REGIONKEY'] for r in regions]

# Compute revenue per nation
results = {}
for item in lineitems:
    l_orderkey = item['L_ORDERKEY']
    if l_orderkey in orders_data:
        s_suppkey = item['L_SUPPKEY']
        if supplier_data.get(s_suppkey) and customer_data.get(orders_data[l_orderkey]):
            n_nationkey = supplier_data[s_suppkey]
            n_regionkey, n_name = nation_data.get(n_nationkey, [None, None])
            if n_regionkey in region_data:
                revenue = item['L_EXTENDEDPRICE'] * (1 - item['L_DISCOUNT'])
                results[n_name] = results.get(n_name, Decimal('0')) + revenue

# Sort the results
sorted_results = sorted(results.items(), key=lambda x: x[1], reverse=True)

# Write the output to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['N_NAME', 'REVENUE'])
    for n_name, revenue in sorted_results:
        writer.writerow([n_name, str(revenue)])
