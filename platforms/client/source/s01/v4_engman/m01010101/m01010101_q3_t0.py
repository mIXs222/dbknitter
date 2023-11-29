import pymysql
import pymongo
import csv

# MySQL connection setup
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# MongoDB connection setup
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Retrieve qualifying orders from MySQL
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute("""
    SELECT O_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY
    FROM orders
    WHERE O_ORDERDATE < '1995-03-05'
""")
orders = mysql_cursor.fetchall()

# Retrieve qualifying customers from MongoDB
customers = list(mongodb_db['customer'].find({'C_MKTSEGMENT': 'BUILDING'}))
customer_keys = [customer['C_CUSTKEY'] for customer in customers]

# Retrieve qualifying lineitems from MongoDB
qualifying_lineitems = list(mongodb_db['lineitem'].find({
    'L_SHIPDATE': {'$gt': '1995-03-15'},
    'L_ORDERKEY': {'$in': [order[0] for order in orders]}
}))

# Calculate revenue for each qualifying order
revenue_by_orderkey = {}
for lineitem in qualifying_lineitems:
    revenue = lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])
    order_key = lineitem['L_ORDERKEY']
    if order_key in revenue_by_orderkey:
        revenue_by_orderkey[order_key] += revenue
    else:
        revenue_by_orderkey[order_key] = revenue

# Merge data and sort by revenue
results = []
for order in orders:
    order_key = order[0]
    order_date = order[1]
    ship_priority = order[2]
    # Only include orders made by customers in the BUILDING segment
    if order_key in revenue_by_orderkey:
        revenue = revenue_by_orderkey[order_key]
        results.append([order_key, revenue, order_date, ship_priority])

results.sort(key=lambda x: x[1], reverse=True)

# Write to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['O_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY'])
    for row in results:
        writer.writerow(row)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()
