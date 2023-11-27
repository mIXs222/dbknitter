# query.py
import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                  user='root',
                                  password='my-secret-pw',
                                  db='tpch')
mysql_cursor = mysql_connection.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_orders = mongo_db['orders']

# Fetch MongoDB data
mongo_pipeline = [
    {"$match": {"O_ORDERDATE": {"$lt": "1995-03-15"}}}
]
orders = list(mongo_orders.aggregate(mongo_pipeline))

# Fetch MySQL data and calculate revenue
mysql_query = """
SELECT
    C_CUSTKEY,
    L_ORDERKEY,
    L_EXTENDEDPRICE,
    L_DISCOUNT
FROM
    customer INNER JOIN lineitem ON C_CUSTKEY = L_ORDERKEY
WHERE
    C_MKTSEGMENT = 'BUILDING'
"""
mysql_cursor.execute(mysql_query)
lineitem_results = mysql_cursor.fetchall()

# Mapping order keys with priority
order_priority_mapping = {}
for order in orders:
    order_priority_mapping[order['O_ORDERKEY']] = {
        'O_ORDERDATE': order['O_ORDERDATE'],
        'O_SHIPPRIORITY': order['O_SHIPPRIORITY']
    }

# Calculate revenue and build results
results = {}
for lineitem in lineitem_results:
    orderkey = lineitem[1]
    if orderkey in order_priority_mapping and order_priority_mapping[orderkey]['O_ORDERDATE'] < '1995-03-15':
        lineitem_revenue = lineitem[2] * (1 - lineitem[3])
        if orderkey in results:
            results[orderkey]['REVENUE'] += lineitem_revenue
        else:
            results[orderkey] = {
                'L_ORDERKEY': orderkey,
                'REVENUE': lineitem_revenue,
                'O_ORDERDATE': order_priority_mapping[orderkey]['O_ORDERDATE'],
                'O_SHIPPRIORITY': order_priority_mapping[orderkey]['O_SHIPPRIORITY']
            }

# Close MySQL connection
mysql_cursor.close()
mysql_connection.close()

# Write results to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    csv_writer = csv.writer(file)
    csv_writer.writerow(['L_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY'])
    
    for row in sorted(results.values(), key=lambda x: (-x['REVENUE'], x['O_ORDERDATE'])):
        csv_writer.writerow([row['L_ORDERKEY'], row['REVENUE'], row['O_ORDERDATE'], row['O_SHIPPRIORITY']])

mongo_client.close()
