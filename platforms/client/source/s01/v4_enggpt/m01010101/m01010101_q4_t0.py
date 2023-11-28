import pymysql
import pymongo
import csv
from datetime import datetime

# MySQL connection
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongodb_conn = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_conn['tpch']
lineitem_collection = mongodb_db['lineitem']

# MySQL query to fetch order details in the given date range
mysql_cursor.execute("""
SELECT O_ORDERPRIORITY, O_ORDERKEY FROM orders 
WHERE O_ORDERDATE BETWEEN '1993-07-01' AND '1993-10-01'
ORDER BY O_ORDERPRIORITY
""")
orders_data = mysql_cursor.fetchall()

# Filter orders by the condition of associated line items in MongoDB
orders_meeting_criteria = {}
for o_priority, o_key in orders_data:
    lineitems = lineitem_collection.find({"L_ORDERKEY": o_key})
    for li in lineitems:
        commit_date = datetime.strptime(li["L_COMMITDATE"], "%Y-%m-%d")
        receipt_date = datetime.strptime(li["L_RECEIPTDATE"], "%Y-%m-%d")
        if commit_date < receipt_date:
            if o_priority not in orders_meeting_criteria:
                orders_meeting_criteria[o_priority] = 0
            orders_meeting_criteria[o_priority] += 1
            break

# Write the results to query_output.csv
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['O_ORDERPRIORITY', 'Order_Count'])
    for priority in sorted(orders_meeting_criteria):
        writer.writerow([priority, orders_meeting_criteria[priority]])

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongodb_conn.close()
