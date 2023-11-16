# mysql_to_mongo_query.py
import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to MySql
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Execute MySQL query for part and orders related information
mysql_query = """
SELECT 
    O_ORDERKEY, 
    strftime('%Y', O_ORDERDATE) AS O_YEAR, 
    O_CUSTKEY, 
    P_PARTKEY, 
    P_TYPE
FROM 
    orders 
JOIN 
    part ON P_PARTKEY = L_PARTKEY 
WHERE 
    O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31' 
    AND P_TYPE = 'SMALL PLATED COPPER'
"""
mysql_cursor.execute(mysql_query)
mysql_results = mysql_cursor.fetchall()

# Filter records in MongoDB by order keys received from MySQL
order_keys = [row[0] for row in mysql_results]
lineitem_data = mongo_db.lineitem.find({'L_ORDERKEY': {'$in': order_keys}})

# Process MongoDB data
lineitem_mapping = {doc['L_ORDERKEY']: doc['L_EXTENDEDPRICE'] * (1 - doc['L_DISCOUNT']) for doc in lineitem_data}

# Get customer and nation information
customers_data = mongo_db.customer.find({'C_CUSTKEY': {'$in': [row[2] for row in mysql_results]}})
customers_mapping = {doc['C_CUSTKEY']: doc for doc in customers_data}
nations_data = mongo_db.nation.find({'N_NATIONKEY': {'$in': [doc['C_NATIONKEY'] for doc in customers_mapping.values()]}})
nations_mapping = {doc['N_NATIONKEY']: doc for doc in nations_data}

# Join and calculate market share
market_share_data = []
for O_ORDERKEY, O_YEAR, O_CUSTKEY, P_PARTKEY, P_TYPE in mysql_results:
    volume = lineitem_mapping.get(O_ORDERKEY, 0)
    cust_doc = customers_mapping.get(O_CUSTKEY, {})
    nation_name = nations_mapping.get(cust_doc.get('C_NATIONKEY', {}), {}).get('N_NAME', '')
    if nation_name == 'INDIA':
        market_share_data.append((O_YEAR, volume))
    else:
        market_share_data.append((O_YEAR, 0))

# Aggregate data by year
yearly_data = {}
for O_YEAR, volume in market_share_data:
    if O_YEAR not in yearly_data:
        yearly_data[O_YEAR] = {'total': 0, 'india': 0}
    yearly_data[O_YEAR]['total'] += volume
    yearly_data[O_YEAR]['india'] += volume

# Write to CSV
with open('query_output.csv', mode='w') as file:
    writer = csv.writer(file)
    writer.writerow(['O_YEAR', 'MKT_SHARE'])
    for O_YEAR, data in sorted(yearly_data.items()):
        writer.writerow([O_YEAR, data['india'] / data['total'] if data['total'] else 0])

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
