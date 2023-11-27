import pymongo
import pymysql
import csv
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
customers = mongo_db['customer']
orders = mongo_db['orders']
lineitems = mongo_db['lineitem']

# Retrieve India and Japan nation keys from MySQL
nation_query = "SELECT N_NATIONKEY FROM nation WHERE N_NAME='INDIA' OR N_NAME='JAPAN'"
mysql_cursor.execute(nation_query)
india_japan_keys = [item[0] for item in mysql_cursor.fetchall()]

# Retrieve customers from MongoDB, which belong to India or Japan
customer_keys = list(customers.find({'C_NATIONKEY': {'$in': india_japan_keys}}, {'C_CUSTKEY': 1, '_id': 0}))

# Get the customer keys
customer_keys = [customer['C_CUSTKEY'] for customer in customer_keys]

# Prepare the final result placeholder
final_results = []

# Find orders made by those customers
for order in orders.find({'O_CUSTKEY': {'$in': customer_keys}}):
    o_orderdate = order['O_ORDERDATE']
    year = datetime.strptime(o_orderdate, '%Y-%m-%d').year
    if year not in [1995, 1996]:
        continue
    o_orderkey = order['O_ORDERKEY']

    # Find lineitems for those orders
    for lineitem in lineitems.find({'L_ORDERKEY': o_orderkey}):
        l_extendedprice = lineitem['L_EXTENDEDPRICE']
        l_discount = lineitem['L_DISCOUNT']
        l_suppkey = lineitem['L_SUPPKEY']

        # Find supplier nation and ensure it is India or Japan
        mysql_cursor.execute("SELECT S_NATIONKEY FROM supplier WHERE S_SUPPKEY=%s", (l_suppkey,))
        supplier_nation = mysql_cursor.fetchone()
        if supplier_nation and supplier_nation[0] in india_japan_keys:
            # Calculate discount and revenue
            revenue = l_extendedprice * (1 - l_discount)

            # Supplier and customer nations
            supplier_nation_key = supplier_nation[0]
            customer_nation_key = order['O_CUSTKEY']

            # Append to final results
            final_results.append((supplier_nation_key, customer_nation_key, year, revenue))

# Map nation keys to nation names
mysql_cursor.execute("SELECT N_NATIONKEY, N_NAME FROM nation")
nation_map = {key: name for key, name in mysql_cursor.fetchall()}

# Convert nation keys to names in final results
for i, result in enumerate(final_results):
    final_results[i] = (
        nation_map[result[0]],
        nation_map[result[1]],
        result[2],
        result[3],
    )

# Sort final results
final_results.sort(key=lambda r: (r[0], r[1], r[2]))

# Write to CSV
with open('query_output.csv', 'w', newline='') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(['Supplier Nation', 'Customer Nation', 'Year', 'Revenue'])
    writer.writerows(final_results)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
