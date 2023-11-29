import pymysql
import pymongo
import pandas as pd
import csv

# Establish connection to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Establish connection to MongoDB
mongodb_client = pymongo.MongoClient(host='mongodb', port=27017)
mongodb_db = mongodb_client['tpch']

# Get nation key for 'SAUDI ARABIA'
nation_key = mongodb_db.nation.find_one({'N_NAME': 'SAUDI ARABIA'}, {'N_NATIONKEY': 1})
supplier_query = {'S_NATIONKEY': nation_key['N_NATIONKEY']}
suppliers = list(mongodb_db.supplier.find(supplier_query, {'S_NAME': 1, 'S_SUPPKEY': 1}))

# Create dict to map supplier key to supplier name and vice versa
suppliers_dict = {str(supplier['S_SUPPKEY']): supplier['S_NAME'] for supplier in suppliers}
suppliers_key_dict = {supplier['S_NAME']: str(supplier['S_SUPPKEY']) for supplier in suppliers}

# Create list of supplier keys
supplier_keys = list(suppliers_dict.keys())

# Get lineitems corresponding to suppliers in 'SAUDI ARABIA'
query = f'''
SELECT COUNT(*) AS NUMWAIT, L.S_SUPPKEY 
FROM lineitem L
JOIN orders O ON L.L_ORDERKEY = O.O_ORDERKEY
WHERE O.O_ORDERSTATUS = 'F'
AND L.L_COMMITDATE < L.L_RECEIPTDATE 
AND L.L_SUPPKEY IN ({','.join(supplier_keys)})
GROUP BY L.S_SUPPKEY
HAVING COUNT(DISTINCT L.L_ORDERKEY) > 1;
'''

mysql_cursor.execute(query)
lineitems_count = mysql_cursor.fetchall()
mysql_conn.close()

# Map supplier key to count and sort by NUMWAIT, then by S_NAME
output_data = [(count, suppliers_dict[supp_key]) for count, supp_key in lineitems_count]
output_data.sort(key=lambda x: (-x[0], x[1]))

# Write output to a CSV file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['NUMWAIT', 'S_NAME'])
    writer.writerows(output_data)
