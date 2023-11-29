# query.py
import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
lineitem_collection = mongo_db['lineitem']

# Get nation key for SAUDI ARABIA
mysql_cursor.execute("SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'SAUDI ARABIA'")
nation_key = mysql_cursor.fetchone()[0]

# Get suppliers from SAUDI ARABIA
mysql_cursor.execute(f"SELECT S_SUPPKEY, S_NAME FROM supplier WHERE S_NATIONKEY = {nation_key}")
suppliers = {supp_key: s_name for supp_key, s_name in mysql_cursor.fetchall()}

# Find orders with status 'F'
mysql_cursor.execute("SELECT O_ORDERKEY FROM orders WHERE O_ORDERSTATUS = 'F'")
orders_with_status_f = {row[0] for row in mysql_cursor.fetchall()}

# Aggregate line items by order key from MongoDB
lineitems_by_order = lineitem_collection.aggregate([
    {"$match": {
        "L_RETURNFLAG": "F",
        "L_ORDERKEY": {"$in": list(orders_with_status_f)}
    }},
    {"$group": {
        "_id": "$L_ORDERKEY",
        "suppliers": {"$addToSet": "$L_SUPPKEY"}
    }}
])

# Determine multi-supplier orders where only one supplier failed to meet delivery
suppliers_awaiting = {}
for item in lineitems_by_order:
    if len(item['suppliers']) > 1:
        for supp in item['suppliers']:
            if supp not in suppliers_awaiting:
                suppliers_awaiting[supp] = 0
            suppliers_awaiting[supp] += 1

# Filter out suppliers not from SAUDI ARABIA
awaiting_sk_sa = {k: suppliers_awaiting[k] for k in suppliers_awaiting if k in suppliers}

# Sort results as per requirement
result = sorted([(num, suppliers[supp_key]) for supp_key, num in awaiting_sk_sa.items()], key=lambda x: (-x[0], x[1]))

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['NUMWAIT', 'S_NAME'])
    for numwait, s_name in result:
        writer.writerow([numwait, s_name])

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
