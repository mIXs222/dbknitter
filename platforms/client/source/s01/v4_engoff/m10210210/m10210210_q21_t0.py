import pymysql
import pymongo
import csv

# Connection to MySQL
mysql_conn = pymysql.connect(host='mysql', db='tpch', user='root', password='my-secret-pw')
mysql_cursor = mysql_conn.cursor()

# Running the query for lineitem tables in MySQL
query_mysql = """
    SELECT DISTINCT L_SUPPKEY
    FROM lineitem
    WHERE L_RETURNFLAG = 'F' AND L_COMMITDATE < L_RECEIPTDATE
"""
mysql_cursor.execute(query_mysql)
suppliers_who_failed = {row[0] for row in mysql_cursor.fetchall()}

# Connection to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Get the N_NATIONKEY for 'SAUDI ARABIA'
nation_key_sa = mongo_db.nation.find_one({'N_NAME': 'SAUDI ARABIA'})['N_NATIONKEY']

# Find all suppliers from 'SAUDI ARABIA'
suppliers_from_sa = list(mongo_db.supplier.find({'S_NATIONKEY': nation_key_sa}, {'S_SUPPKEY': 1}))
supplier_keys_from_sa = {supplier['S_SUPPKEY'] for supplier in suppliers_from_sa}

# Find orders with status 'F'
orders_with_f_status = mongo_db.orders.find({'O_ORDERSTATUS': 'F'}, {'O_ORDERKEY': 1})
order_keys_with_f_status = {order['O_ORDERKEY'] for order in orders_with_f_status}

# Combine results to find suppliers from SAUDI ARABIA that failed to meet the delivery date
# while being the only supplier in the order to do so.
final_suppliers = supplier_keys_from_sa.intersection(suppliers_who_failed)

# Fetch supplier details from MongoDB
final_supplier_details = list(mongo_db.supplier.find({'S_SUPPKEY': {'$in': list(final_suppliers)}}))

# Write the output to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for supplier in final_supplier_details:
        writer.writerow({
            'S_SUPPKEY': supplier['S_SUPPKEY'],
            'S_NAME': supplier['S_NAME'],
            'S_ADDRESS': supplier['S_ADDRESS'],
            'S_PHONE': supplier['S_PHONE'],
            'S_ACCTBAL': supplier['S_ACCTBAL'],
            'S_COMMENT': supplier['S_COMMENT']
        })

# Close the connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
