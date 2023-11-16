import pymongo
import pymysql
import csv

# MySQL Connection
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# MongoDB Connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Retrieve necessary data from MySQL
mysql_cursor = mysql_connection.cursor()
mysql_cursor.execute("""
SELECT nation.N_NATIONKEY, N_NAME
FROM nation
WHERE N_NAME = 'SAUDI ARABIA'
""")
nation_result = mysql_cursor.fetchall()

N_NATIONKEY = [n[0] for n in nation_result]
mysql_cursor.execute("""
SELECT O_ORDERKEY, O_ORDERSTATUS
FROM orders
WHERE O_ORDERSTATUS = 'F'
""")
orders_result = mysql_cursor.fetchall()

O_ORDERKEY_WITH_STATUS_F = [o[0] for o in orders_result]

# Close MySQL connection
mysql_cursor.close()
mysql_connection.close()

# Retrieve necessary data from MongoDB
supplier_data = list(mongodb.supplier.find({'S_NATIONKEY': {'$in': N_NATIONKEY}}, {'_id': 0, 'S_SUPPKEY': 1, 'S_NAME': 1}))
lineitem_data = list(mongodb.lineitem.find(
    {
        '$expr': {
            '$and': [
                {'$in': ['$L_ORDERKEY', O_ORDERKEY_WITH_STATUS_F]},
                {'$gt': ['$L_RECEIPTDATE', '$L_COMMITDATE']}
            ]
        }
    },
    {'_id': 0, 'L_ORDERKEY': 1, 'L_SUPPKEY': 1}
))

# Additional filter using Python since MongoDB can't easily reproduce the entire SQL logic, especially subqueries
orderkey_to_suppkeys = {}
for lineitem in lineitem_data:
    orderkey_to_suppkeys.setdefault(lineitem['L_ORDERKEY'], set()).add(lineitem['L_SUPPKEY'])

# Assemble the final result using Python combining logic
final_result = []
for supplier in supplier_data:
    suppkey = supplier['S_SUPPKEY']
    numwait = 0
    for orderkey, suppkeys in orderkey_to_suppkeys.items():
        if suppkey in suppkeys and any(other != suppkey for other in suppkeys):
            all_received_late = all(other != suppkey and other not in orderkey_to_suppkeys.keys() for other in suppkeys)
            if all_received_late:
                numwait += 1
    if numwait > 0:
        final_result.append((supplier['S_NAME'], numwait))

# Sort the result as per the original SQL ORDER BY clause
sorted_result = sorted(final_result, key=lambda x: (-x[1], x[0]))

# Write the output to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['S_NAME', 'NUMWAIT'])
    for row in sorted_result:
        csvwriter.writerow(row)
