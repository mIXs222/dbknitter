import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)
mysql_cursor = mysql_conn.cursor()

# Execute query for MySQL part (supplier and nation)
mysql_query = """
SELECT S_NAME, S_SUPPKEY, S_NATIONKEY
FROM supplier, nation
WHERE S_NATIONKEY = N_NATIONKEY
AND N_NAME = 'SAUDI ARABIA'
"""
mysql_cursor.execute(mysql_query)
supplier_data = {row[1]: row[0] for row in mysql_cursor.fetchall() if row[2] in supplier_data}

# Now close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017, serverSelectionTimeoutMS=5000)
mongo_db = mongo_client["tpch"]
orders_collection = mongo_db["orders"]
lineitem_collection = mongo_db["lineitem"]

# This nested dict will hold the counts: {supplier_name: {order_key: bool}}
supplier_wait_counts = {supplier_name: {} for supplier_name in supplier_data.values()}

# Query for MongoDB part (orders and lineitems)
mongo_query = {
    'O_ORDERSTATUS': 'F',
}
orders_cursor = orders_collection.find(mongo_query, {'O_ORDERKEY': 1})

# Process orders and lineitems
for order in orders_cursor:
    order_key = order['O_ORDERKEY']
    lineitems_cursor = lineitem_collection.find({'L_ORDERKEY': order_key})

    for lineitem in lineitems_cursor:
        if lineitem['L_SUPPKEY'] in supplier_data and lineitem['L_RECEIPTDATE'] > lineitem['L_COMMITDATE']:
            if order_key not in supplier_wait_counts[supplier_data[lineitem['L_SUPPKEY']]]:
                supplier_wait_counts[supplier_data[lineitem['L_SUPPKEY']]][order_key] = True

            # Check for the NOT EXISTS condition using another lineitem cursor
            other_lineitems_cursor = lineitem_collection.find({
                'L_ORDERKEY': order_key,
                'L_SUPPKEY': {'$ne': lineitem['L_SUPPKEY']},
                'L_RECEIPTDATE': {'$gt': lineitem['L_COMMITDATE']},
            })
            if other_lineitems_cursor.count() > 0:
                supplier_wait_counts[supplier_data[lineitem['L_SUPPKEY']]][order_key] = False

    # Filter out order_keys where the condition wasn't met
    for supp_name, order_dict in supplier_wait_counts.items():
        supplier_wait_counts[supp_name] = {k: v for k, v in order_dict.items() if v}

# Calculate NUMWAIT and sort by NUMWAIT DESC, S_NAME
final_result = [{'S_NAME': supp_name, 'NUMWAIT': len(order_dict)} for supp_name, order_dict in supplier_wait_counts.items()]
final_result.sort(key=lambda x: (-x['NUMWAIT'], x['S_NAME']))

# Write to CSV
with open('query_output.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['S_NAME', 'NUMWAIT'])
    writer.writeheader()
    for row in final_result:
        writer.writerow(row)

# Close MongoDB connection
mongo_client.close()
