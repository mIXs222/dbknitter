import pymysql
import pymongo
import csv
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_supplier = mongo_db['supplier']

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Run the SQL query on MySQL for lineitem and orders table
mysql_cursor.execute("""
SELECT
    L1.L_ORDERKEY AS L_ORDERKEY,
    L1.L_SUPPKEY AS L_SUPPKEY,
    L1.L_RECEIPTDATE,
    L1.L_COMMITDATE,
    O_ORDERSTATUS
FROM
    lineitem AS L1
JOIN
    orders ON O_ORDERKEY = L1.L_ORDERKEY
WHERE
    O_ORDERSTATUS = 'F'
    AND L1.L_RECEIPTDATE > L1.L_COMMITDATE
""")
orders_lineitem_result = mysql_cursor.fetchall()

# get all nations from Redis
nation = redis_conn.get('nation')

# Filter suppliers from SAUDI ARABIA
saudi_suppliers = []
for supplier in mongo_supplier.find({"S_NATIONKEY": nation['SAUDI ARABIA']}):
    saudi_suppliers.append(supplier)

# Apply the EXISTS conditions, filter suppliers and count orders
supplier_order_count = {}
for order in orders_lineitem_result:
    # Check the EXISTS subquery condition
    mysql_cursor.execute("""
    SELECT EXISTS(
        SELECT *
        FROM lineitem AS L2
        WHERE L2.L_ORDERKEY = %s AND L2.L_SUPPKEY <> %s
    )
    """, (order['L_ORDERKEY'], order['L_SUPPKEY']))
    exists_condition = mysql_cursor.fetchone()[0]

    # Check the NOT EXISTS subquery condition
    mysql_cursor.execute("""
    SELECT NOT EXISTS(
        SELECT *
        FROM lineitem AS L3
        WHERE L3.L_ORDERKEY = %s AND L3.L_SUPPKEY <> %s AND L3.L_RECEIPTDATE > L3.L_COMMITDATE
    )
    """, (order['L_ORDERKEY'], order['L_SUPPKEY']))
    not_exists_condition = mysql_cursor.fetchone()[0]

    if exists_condition and not_exists_condition:
        for supp in saudi_suppliers:
            if order['L_SUPPKEY'] == supp['S_SUPPKEY']:
                supplier_order_count[supp['S_NAME']] = supplier_order_count.get(supp['S_NAME'], 0) + 1
    
# Sort suppliers by NUMWAIT and S_NAME
sorted_suppliers = sorted(supplier_order_count.items(), key=lambda x: (-x[1], x[0]))

# Write output to a CSV file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['S_NAME', 'NUMWAIT'])
    for supplier_name, numwait in sorted_suppliers:
        writer.writerow([supplier_name, numwait])

# Close all connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
redis_conn.close()
