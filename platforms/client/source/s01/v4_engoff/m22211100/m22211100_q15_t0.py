import pymysql
import pymongo
import csv
import datetime

# MySQL Connection
mysql_conn = pymysql.connect(
    host='mysql', 
    user='root', 
    password='my-secret-pw', 
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()

# MongoDB Connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
supplier_collection = mongo_db['supplier']

# Query MySQL for lineitem data higher than '1996-01-01' and lower or equal than '1996-04-01'
mysql_cursor.execute("""SELECT L_SUPPKEY, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS revenue
                        FROM lineitem
                        WHERE L_SHIPDATE >= '1996-01-01' AND L_SHIPDATE <= '1996-04-01'
                        GROUP BY L_SUPPKEY
                        ORDER BY revenue DESC, L_SUPPKEY;""")

# Get the rows and find the highest revenue
rows = mysql_cursor.fetchall()
max_revenue = rows[0][1] if rows else 0

# Find supplier with revenue equal to the highest revenue 
top_suppliers = [row[0] for row in rows if row[1] == max_revenue]

# Find supplier details from MongoDB
top_suppliers_details = list(supplier_collection.find({'S_SUPPKEY': {'$in': top_suppliers}}))

# Sort the supplier details based on the supplier key
sorted_suppliers_details = sorted(top_suppliers_details, key=lambda k: k['S_SUPPKEY'])

# Write the output to a CSV file
with open('query_output.csv', mode='w') as file:
    writer = csv.writer(file)
    writer.writerow(['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])
    for supplier in sorted_suppliers_details:
        writer.writerow([
            supplier['S_SUPPKEY'], supplier['S_NAME'], supplier['S_ADDRESS'], 
            supplier['S_NATIONKEY'], supplier['S_PHONE'], supplier['S_ACCTBAL'], supplier['S_COMMENT']
        ])

# Close the connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
