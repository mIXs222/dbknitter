import pymysql
import pymongo
import csv
from datetime import datetime

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Extract parts with 'dim' in their name from mysql
part_query = "SELECT P_PARTKEY FROM part WHERE P_NAME LIKE '%dim%'"
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute(part_query)
part_keys = [row[0] for row in mysql_cursor.fetchall()]

# Extract supplier info from mongodb
supplier_dict = {doc['S_SUPPKEY']: doc['S_NATIONKEY'] for doc in mongodb['supplier'].find()}

# Analyze data
output_data = []

# Process line items from mongodb
for lineitem in mongodb['lineitem'].find({'L_PARTKEY': {'$in': part_keys}}):
    if lineitem['L_SUPPKEY'] in supplier_dict:
        nation_key = supplier_dict[lineitem['L_SUPPKEY']]
        order_date_query = f"SELECT O_ORDERDATE, N_NAME FROM orders, nation WHERE O_ORDERKEY = {lineitem['L_ORDERKEY']} AND nation.N_NATIONKEY = {nation_key}"
        mysql_cursor.execute(order_date_query)
        order_date, nation_name = mysql_cursor.fetchone()

        year = datetime.strptime(order_date, '%Y-%m-%d').year
        supply_cost_query = f"SELECT PS_SUPPLYCOST FROM partsupp WHERE PS_PARTKEY = {lineitem['L_PARTKEY']} AND PS_SUPPKEY = {lineitem['L_SUPPKEY']}"
        mysql_cursor.execute(supply_cost_query)
        supply_cost = mysql_cursor.fetchone()[0]

        profit = (lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])) - (supply_cost * lineitem['L_QUANTITY'])
        
        output_data.append((nation_name, year, profit))

mysql_cursor.close()
mysql_conn.close()

# Sort data by nation ascending and year descending
output_data.sort(key=lambda x: (x[0], -x[1]))

# Write the data to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Nation', 'Year', 'Profit'])
    for row in output_data:
        writer.writerow(row)
