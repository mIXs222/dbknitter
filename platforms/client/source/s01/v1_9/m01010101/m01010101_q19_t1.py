import csv
import mysql.connector
from pymongo import MongoClient

# Connect to MySQL
mysql_conn = mysql.connector.connect(user='root', password='my-secret-pw', host='mysql', database='tpch')

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Get data from MySQL
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute('SELECT * FROM PART')
mysql_data = mysql_cursor.fetchall()
mysql_cursor.close()

# Convert MySQL data to dictionary for easier processing
part_dict = {}
for row in mysql_data:
    part_dict[row[0]] = row

# Get data from MongoDB
lineitem_data = list(mongo_db.lineitem.find())

# Process data
result = 0
for lineitem in lineitem_data:
    part = part_dict.get(lineitem['L_PARTKEY'])
    if part:
        conditions = [
            (part[3] == 'Brand#12' and part[6] in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') and 1 <= lineitem['L_QUANTITY'] <= 11 and 1 <= part[5] <= 5),
            (part[3] == 'Brand#23' and part[6] in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') and 10 <= lineitem['L_QUANTITY'] <= 20 and 1 <= part[5] <= 10),
            (part[3] == 'Brand#34' and part[6] in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') and 20 <= lineitem['L_QUANTITY'] <= 30 and 1 <= part[5] <= 15)
        ]
        if any(conditions) and lineitem['L_SHIPMODE'] in ('AIR', 'AIR REG') and lineitem['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON':
            result += lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])

# Write output to CSV file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['REVENUE'])
    writer.writerow([result])
