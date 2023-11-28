import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Prepare the query for MySQL
mysql_query = """
SELECT c.C_CUSTKEY, c.C_NAME, o.O_ORDERKEY, l.L_EXTENDEDPRICE, l.L_DISCOUNT
FROM customer c 
JOIN lineitem l ON c.C_CUSTKEY = l.L_ORDERKEY 
WHERE c.C_MKTSEGMENT = 'BUILDING'
"""

# Prepare the date criteria
date_criteria = datetime(1995, 3, 15)

# Execute MySQL query
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute(mysql_query)
mysql_results = mysql_cursor.fetchall()

# Fetching orders from MongoDB
mongo_orders = list(mongodb['orders'].find({
    'O_ORDERDATE': {'$lt': date_criteria},
    'O_ORDERSTATUS': {'$ne': 'BUILDING'}
}))

# Combine results
combined_results = []
for order in mongo_orders:
    for row in mysql_results:
        if row[0] == order['O_CUSTKEY']:
            extended_price = row[3]
            discount = row[4]
            revenue = extended_price * (1 - discount)
            combined_results.append({
                'O_ORDERKEY': order['O_ORDERKEY'],
                'O_ORDERDATE': order['O_ORDERDATE'],
                'O_SHIPPRIORITY': order['O_SHIPPRIORITY'],
                'REVENUE': revenue
            })

# Sort combined results
sorted_results = sorted(combined_results, key=lambda x: (-x['REVENUE'], x['O_ORDERDATE']))

# Writing results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY', 'REVENUE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for result in sorted_results:
        writer.writerow(result)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
