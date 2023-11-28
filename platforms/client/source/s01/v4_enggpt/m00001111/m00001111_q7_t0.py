# import necessary packages
import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to MySQL database
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB database
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Query the nation table in MySQL for 'JAPAN' and 'INDIA'
nation_query = "SELECT N_NATIONKEY, N_NAME FROM nation WHERE N_NAME='JAPAN' OR N_NAME='INDIA'"
mysql_cursor.execute(nation_query)
nation_keys = {row[0]: row[1] for row in mysql_cursor.fetchall()}

# Find the respective nation keys for 'JAPAN' and 'INDIA'
japan_nation_key = next(key for key, value in nation_keys.items() if value == 'JAPAN')
india_nation_key = next(key for key, value in nation_keys.items() if value == 'INDIA')

# Query the supplier table in MySQL
supplier_query = "SELECT S_SUPPKEY, S_NATIONKEY FROM supplier WHERE S_NATIONKEY IN (%s, %s)"
mysql_cursor.execute(supplier_query, (japan_nation_key, india_nation_key))
supplier_keys = {row[0]: row[1] for row in mysql_cursor.fetchall()}

# Query customer and orders in MongoDB
customers = mongo_db['customer'].find({'C_NATIONKEY': {'$in': [japan_nation_key, india_nation_key]}})
customer_orders = {}
for customer in customers:
    orders = mongo_db['orders'].find({'O_CUSTKEY': customer['C_CUSTKEY']})
    for order in orders:
        order_date = datetime.strptime(order['O_ORDERDATE'], '%Y-%m-%d')  # Assuming the date format is known
        if datetime(1995, 1, 1) <= order_date <= datetime(1996, 12, 31):
            customer_orders[order['O_ORDERKEY']] = {
                'customer_nation': nation_keys[customer['C_NATIONKEY']],
                'year': order_date.year
            }

# Query lineitem in MongoDB and calculate revenue
lineitems = mongo_db['lineitem'].find({'L_ORDERKEY': {'$in': list(customer_orders.keys())}})
revenue_volume_data = []
for item in lineitems:
    if item['L_SUPPKEY'] in supplier_keys:
        supp_nation = nation_keys[supplier_keys[item['L_SUPPKEY']]]
        cust_nation = customer_orders[item['L_ORDERKEY']]['customer_nation']
        if supp_nation != cust_nation:
            volume = item['L_EXTENDEDPRICE'] * (1 - item['L_DISCOUNT'])
            revenue_volume_data.append({
                'supplier_nation': supp_nation,
                'customer_nation': cust_nation,
                'year': customer_orders[item['L_ORDERKEY']]['year'],
                'revenue': volume
            })

# Sort the revenue data
revenue_volume_data.sort(key=lambda x: (x['supplier_nation'], x['customer_nation'], x['year']))

# Write output to CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    # Write the headers to the CSV file
    writer.writerow(['supplier_nation', 'customer_nation', 'year', 'revenue'])
    # Write the data to the CSV file
    for row in revenue_volume_data:
        writer.writerow([row['supplier_nation'], row['customer_nation'], row['year'], row['revenue']])

# Close database connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
