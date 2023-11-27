import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to MySQL database
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB database
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Read necessary data from MySQL tables
with mysql_connection.cursor() as cursor:
    cursor.execute("SELECT S_SUPPKEY, S_NATIONKEY FROM supplier")
    supplier = cursor.fetchall()
    cursor.execute("SELECT L_ORDERKEY, L_SUPPKEY, L_EXTENDEDPRICE, L_DISCOUNT, L_SHIPDATE FROM lineitem WHERE L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'")
    lineitem = cursor.fetchall()
    cursor.execute("SELECT C_CUSTKEY, C_NATIONKEY FROM customer")
    customer = cursor.fetchall()

# Read necessary data from MongoDB collections
nation = list(mongodb.nation.find({}, {'_id': 0}))
orders = list(mongodb.orders.find({}, {'_id': 0}))

# Convert to dictionary for efficient lookups
supplier_dict = {s[0]: s[1] for s in supplier}
customer_dict = {c[0]: c[1] for c in customer}
nation_dict = {n['N_NATIONKEY']: n['N_NAME'] for n in nation}
orders_dict = {o['O_ORDERKEY']: o['O_CUSTKEY'] for o in orders}

# Prepare the collected data for join operations
joint_data = []
for l in lineitem:
    s_nationkey = supplier_dict.get(l[1])
    o_custkey = orders_dict.get(l[0])
    if s_nationkey is None or o_custkey is None:
        continue
    c_nationkey = customer_dict.get(o_custkey)
    if c_nationkey is None:
        continue
    sup_nation = nation_dict.get(s_nationkey)
    cust_nation = nation_dict.get(c_nationkey)
    if (sup_nation == 'JAPAN' and cust_nation == 'INDIA') or (sup_nation == 'INDIA' and cust_nation == 'JAPAN'):
        volume = l[2] * (1 - l[3])
        l_year = datetime.strptime(l[4], '%Y-%m-%d').year
        joint_data.append((sup_nation, cust_nation, l_year, volume))

# Group by SUPP_NATION, CUST_NATION, L_YEAR and calculate REVENUE
result = {}
for row in joint_data:
    key = (row[0], row[1], row[2])
    result[key] = result.get(key, 0) + row[3]

# Sort the result and write to CSV file
result_sorted = sorted(result.items(), key=lambda x: (x[0][0], x[0][1], x[0][2]))
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['SUPP_NATION', 'CUST_NATION', 'L_YEAR', 'REVENUE'])
    for key, revenue in result_sorted:
        csvwriter.writerow([key[0], key[1], key[2], revenue])

# Close the database connections
mysql_connection.close()
mongo_client.close()
