# execute_query.py
import pymongo
import pymysql
import csv

# Establish MySQL connection
mysql_connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_connection.cursor()

# Establish MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
orders_collection = mongodb['orders']

# Retrieve orders from MongoDB where O_ORDERDATE is within the specified range
order_keys_within_range = []
for order in orders_collection.find({"O_ORDERDATE": {"$gte": "1993-07-01", "$lt": "1993-10-01"}}):
    order_keys_within_range.append(order['O_ORDERKEY'])

# Prepare the SQL query with EXISTS clause
sql_query = """
SELECT
    L_ORDERKEY
FROM
    lineitem
WHERE
    L_COMMITDATE < L_RECEIPTDATE
"""
mysql_cursor.execute(sql_query)

# Retrieve L_ORDERKEY from lineitem where L_COMMITDATE < L_RECEIPTDATE
valid_lineitem_orderkeys = set(result[0] for result in mysql_cursor.fetchall())

# Find the common order keys between our valid lineitem orderkeys and the range-filtered order keys from mongodb
common_order_keys = [order_key for order_key in order_keys_within_range if order_key in valid_lineitem_orderkeys]

# Now get the final count of orders for each O_ORDERPRIORITY, since we know which order keys are valid
final_query = f"""
SELECT
    O_ORDERPRIORITY, COUNT(*) AS ORDER_COUNT
FROM
    orders
WHERE
    O_ORDERKEY IN ({','.join(map(str, common_order_keys))})
GROUP BY
    O_ORDERPRIORITY
ORDER BY
    O_ORDERPRIORITY
"""
mysql_cursor.execute(final_query)

# Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(['O_ORDERPRIORITY', 'ORDER_COUNT'])  # Write headers
    for row in mysql_cursor:
        csv_writer.writerow(row)

# Close connections
mysql_cursor.close()
mysql_connection.close()
mongo_client.close()
