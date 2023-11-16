# python code (query_execution.py)

import pymysql
import pymongo
import csv
from datetime import datetime

# Establish connection to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    port=3306,
    user='root',
    password='my-secret-pw',
    database='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Fetch MySQL data
mysql_cursor.execute("""
SELECT nation.N_NATIONKEY, N_NAME, P_PARTKEY, P_NAME, S_SUPPKEY, S_NATIONKEY
FROM nation
JOIN supplier ON nation.N_NATIONKEY = supplier.S_NATIONKEY
JOIN part ON P_NAME LIKE '%dim%'
""")
mysql_data = mysql_cursor.fetchall()

# Create mapping for later lookup
supplier_nation = {row[4]: (row[0], row[1]) for row in mysql_data if row[4] is not None}
part_keys = {row[2] for row in mysql_data if row[2] is not None and row[3] is not None}

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Fetch MongoDB data
partsupp_collection = mongodb_db['partsupp']
lineitem_collection = mongodb_db['lineitem']
orders_collection = mongodb_db['orders']

partsupp_data = list(partsupp_collection.find(
    {'PS_PARTKEY': {'$in': list(part_keys)}},
    {'PS_PARTKEY': 1, 'PS_SUPPKEY': 1, 'PS_SUPPLYCOST': 1}
))

lineitem_data = list(lineitem_collection.find(
    {'L_PARTKEY': {'$in': list(part_keys)}},
    {'L_ORDERKEY': 1, 'L_PARTKEY': 1, 'L_SUPPKEY': 1, 'L_EXTENDEDPRICE': 1, 'L_DISCOUNT': 1, 'L_QUANTITY': 1}
))

orders_data = list(orders_collection.find(
    {},
    {'O_ORDERKEY': 1, 'O_ORDERDATE': 1}
))

# Define the result list
results = []

# Process the data to simulate the query
for li in lineitem_data:
    ps_data = [ps for ps in partsupp_data if ps['PS_PARTKEY'] == li['L_PARTKEY'] and ps['PS_SUPPKEY'] == li['L_SUPPKEY']]
    if not ps_data:
        continue
    ps_data = ps_data[0]
    order_data = [o for o in orders_data if o['O_ORDERKEY'] == li['L_ORDERKEY']]
    
    if not order_data:
        continue
    order_data = order_data[0]

    nation_info = supplier_nation.get(li['L_SUPPKEY'])
    if not nation_info:
        continue

    nation_key, nation_name = nation_info
    o_year = datetime.strptime(order_data['O_ORDERDATE'], '%Y-%m-%d').year
    amount = float(li['L_EXTENDEDPRICE']) * (1 - float(li['L_DISCOUNT'])) - float(ps_data['PS_SUPPLYCOST']) * li['L_QUANTITY']
    results.append((nation_name, o_year, amount))

# Group by and sum the amount
final_results = {}
for nation, o_year, amount in results:
    final_results.setdefault((nation, o_year), 0)
    final_results[(nation, o_year)] += amount

# Write results to a CSV
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['NATION', 'O_YEAR', 'SUM_PROFIT'])
    for (nation, o_year), sum_profit in sorted(final_results.items()):
        writer.writerow([nation, o_year, sum_profit])
