# order_priority_checking.py

import pymysql
import pymongo
import csv

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
)

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
orders_collection = mongo_db['orders']

# Retrieve orders IDs from Mongo whose order date is within the specified range
query_date_filter = {
    '$and': [
        {'O_ORDERDATE': {'$gte': '1993-07-01'}},
        {'O_ORDERDATE': {'$lte': '1993-10-01'}}
    ]
}
orders_cursor = orders_collection.find(query_date_filter)
order_keys = [order['O_ORDERKEY'] for order in orders_cursor]

# Retrieve lineitem information for the obtained orders from MySQL
result = {}
with mysql_conn.cursor() as cursor:
    lineitem_query = """
    SELECT
        L_ORDERKEY,
        COUNT(DISTINCT L_ORDERKEY) as ORDER_COUNT,
        L_COMMITDATE,
        L_RECEIPTDATE
    FROM
        lineitem
    WHERE
        L_ORDERKEY IN (%s)
    GROUP BY
        L_ORDERKEY
    HAVING
        MAX(L_COMMITDATE) < MAX(L_RECEIPTDATE);
    """
    format_strings = ','.join(['%s'] * len(order_keys))
    cursor.execute(lineitem_query % format_strings, tuple(order_keys))
    for row in cursor:
        if row[3] > row[2]:
            if row[0] in result:
                result[row[0]]['ORDER_COUNT'] += 1
            else:
                result[row[0]] = {'ORDER_COUNT': 1}

# Update count for orders from retrieved line items and get order priority
for order_key in result:
    order_query = {'O_ORDERKEY': order_key}
    order_info = orders_collection.find_one(order_query, {'_id': 0, 'O_ORDERPRIORITY': 1})
    result[order_key]['O_ORDERPRIORITY'] = order_info['O_ORDERPRIORITY']

# Aggregate counts by order priority
final_result = {}
for r in result.values():
    priority = r['O_ORDERPRIORITY']
    if priority in final_result:
        final_result[priority] += 1
    else:
        final_result[priority] = 1

# Write the results to a CSV file
with open('query_output.csv', mode='w', newline='') as output_file:
    writer = csv.writer(output_file)
    writer.writerow(['ORDER_COUNT', 'O_ORDERPRIORITY'])
    for priority, count in sorted(final_result.items()):
        writer.writerow([count, priority])

# Close connections
mysql_conn.close()
mongo_client.close()
