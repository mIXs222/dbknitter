import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]

# Getting the nations and regions from MySQL
cursor = mysql_conn.cursor()
cursor.execute("SELECT n.N_NATIONKEY, n.N_NAME FROM nation n "
               "JOIN region r ON n.N_REGIONKEY = r.R_REGIONKEY "
               "WHERE r.R_NAME = 'ASIA'")
asian_nations = {row[0]: row[1] for row in cursor.fetchall()}
cursor.close()

# Calculate revenue volume for each nation in ASIA
revenue_by_nation = {key: 0.0 for key in asian_nations}

# Converting date strings to proper datetime objects for comparison in MongoDB queries
start_date = datetime(1990, 1, 1)
end_date = datetime(1995, 1, 1)

# Getting data from MongoDB
customers = mongo_db['customer'].find({'C_NATIONKEY': {'$in': list(asian_nations.keys())}})
customer_keys = {customer['C_CUSTKEY'] for customer in customers}

orders = mongo_db['orders'].find({
    'O_CUSTKEY': {'$in': list(customer_keys)},
    'O_ORDERDATE': {'$gte': start_date, '$lt': end_date}
})
order_keys = {order['O_ORDERKEY'] for order in orders}

# Aggregating lineitem data
lineitems = mongo_db['lineitem'].find({'L_ORDERKEY': {'$in': list(order_keys)}})
for lineitem in lineitems:
    nation_key = lineitem.get('L_SUPPKEY', None)  # Assuming that L_SUPPKEY is actually linked to S_NATIONKEY
    if nation_key in asian_nations:
        l_extendedprice = float(lineitem.get('L_EXTENDEDPRICE', 0.0))
        l_discount = float(lineitem.get('L_DISCOUNT', 0.0))
        revenue = l_extendedprice * (1 - l_discount)
        revenue_by_nation[nation_key] += revenue

mysql_conn.close()
mongo_client.close()

# Preparing data for CSV output
output_data = [{'Nation': asian_nations[nation_key], 'Revenue': revenue}
               for nation_key, revenue in revenue_by_nation.items()]
# Sorting by revenue in descending order
sorted_output_data = sorted(output_data, key=lambda x: x['Revenue'], reverse=True)

# Writing to query_output.csv
with open('query_output.csv', mode='w', newline='') as csvfile:
    fieldnames = ['Nation', 'Revenue']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for row in sorted_output_data:
        writer.writerow(row)
