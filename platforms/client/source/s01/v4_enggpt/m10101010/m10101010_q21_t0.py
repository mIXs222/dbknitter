# import required modules
import pymysql
import pymongo
import csv

# Connect to the MySQL database
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to the MongoDB database
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_nation = mongo_db['nation']
mongo_orders = mongo_db['orders']

# Query to get the suppliers in Saudi Arabia from MySQL database
mysql_cursor.execute("""
    SELECT S_SUPPKEY, S_NAME
    FROM supplier
    WHERE S_NATIONKEY = (
        SELECT N_NATIONKEY
        FROM nation
        WHERE N_NAME = 'SAUDI ARABIA'
    )
""")
saudi_suppliers = {row[0]: row[1] for row in mysql_cursor.fetchall()}

# Query to get the line items from MySQL database
mysql_cursor.execute("""
    SELECT L_SUPPKEY, L_ORDERKEY
    FROM lineitem L1
    WHERE L_RECEIPTDATE > L_COMMITDATE
    AND EXISTS (
        SELECT 1
        FROM orders
        WHERE O_ORDERKEY = L1.L_ORDERKEY
        AND O_ORDERSTATUS = 'F'
    )
    AND NOT EXISTS (
        SELECT 1
        FROM lineitem L2
        WHERE L2.L_ORDERKEY = L1.L_ORDERKEY
        AND L2.L_SUPPKEY != L1.L_SUPPKEY
        AND L2.L_RECEIPTDATE > L2.L_COMMITDATE
    )
""")
line_items = mysql_cursor.fetchall()

# Create a dictionary to count waiting time per supplier
supplier_wait_times = {s_key: 0 for s_key in saudi_suppliers.keys()}

# Iterate through line items and increase the waiting time for the corresponding supplier
for l_suppkey, l_orderkey in line_items:
    if l_suppkey in supplier_wait_times:
        # Fetch order status from MongoDB to make sure status is 'F'
        order_status = mongo_orders.find_one({'O_ORDERKEY': l_orderkey}, {'O_ORDERSTATUS': 1})
        if order_status and order_status['O_ORDERSTATUS'] == 'F':
            supplier_wait_times[l_suppkey] += 1

# Sort the results based on waiting time and then supplier name
sorted_results = sorted(
    [(saudi_suppliers[s_key], count) for s_key, count in supplier_wait_times.items() if count > 0],
    key=lambda k: (-k[1], k[0])
)

# Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['S_NAME', 'NUMWAIT'])
    for result in sorted_results:
        writer.writerow(result)

# Close the connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
