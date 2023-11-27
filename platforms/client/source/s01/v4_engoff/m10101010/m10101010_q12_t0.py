import csv
import pymysql
import pymongo
from datetime import datetime

# MongoDB connection setup
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# MySQL connection setup
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')
mysql_cursor = mysql_conn.cursor()

# Function to get orders with priority URGENT or HIGH from MongoDB
def get_orders_with_priority():
    urgent_high_orders = mongo_db.orders.find(
        {
            'O_ORDERDATE': {'$gte': datetime(1994, 1, 1), '$lte': datetime(1995, 1, 1)},
            'O_ORDERPRIORITY': {'$in': ['URGENT', 'HIGH']}
        },
        {'O_ORDERKEY': 1}
    )
    return set(order['O_ORDERKEY'] for order in urgent_high_orders)

# Query MySQL and MongoDB databases
urgent_high_order_keys = get_orders_with_priority()

query = """
SELECT L_SHIPMODE, COUNT(*) AS late_count, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS revenue_loss
FROM lineitem
WHERE L_SHIPMODE IN ('MAIL', 'SHIP')
  AND L_SHIPDATE < L_COMMITDATE
  AND L_RECEIPTDATE > L_COMMITDATE
  AND L_RECEIPTDATE BETWEEN '1994-01-01' AND '1995-01-01'
GROUP BY L_SHIPMODE
"""

mysql_cursor.execute(query)
results = mysql_cursor.fetchall()

# Writing output to query_output.csv
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['ship_mode', 'late_count', 'revenue_loss'])

    for lineitem in results:
        order_key = lineitem[0]
        is_urgent_high = order_key in urgent_high_order_keys
        ship_mode, late_count, revenue_loss = lineitem
        if is_urgent_high:
            writer.writerow([ship_mode, 'URGENT/HIGH', late_count, revenue_loss])
        else:
            writer.writerow([ship_mode, 'OTHER', late_count, revenue_loss])

# Close connections
mysql_conn.close()
mongo_client.close()
