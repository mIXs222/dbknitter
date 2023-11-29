import pymysql
import pymongo
import csv

# Connect to the MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)
mysql_cursor = mysql_conn.cursor()

# Connect to the MongoDB database
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
customer_collection = mongo_db['customer']

# Find customers in the 'BUILDING' market segment
building_customers = list(customer_collection.find({'C_MKTSEGMENT': 'BUILDING'}, {'C_CUSTKEY': 1}))

# Extract customer keys
building_custkeys = [customer['C_CUSTKEY'] for customer in building_customers]

# Perform SQL query on MySQL database
query = """
SELECT 
    o.O_ORDERKEY,
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) as REVENUE,
    o.O_ORDERDATE,
    o.O_SHIPPRIORITY
FROM orders o
JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
WHERE o.O_CUSTKEY IN (%s)
  AND o.O_ORDERDATE < '1995-03-05'
  AND l.L_SHIPDATE > '1995-03-15'
GROUP BY o.O_ORDERKEY, o.O_ORDERDATE, o.O_SHIPPRIORITY
ORDER BY REVENUE DESC
"""
# We need to format the query by joining customer keys to include them in the IN clause
formatted_customer_keys = ', '.join(str(custkey) for custkey in building_custkeys)
formatted_query = query % formatted_customer_keys

mysql_cursor.execute(formatted_query)

# Get the results
results = mysql_cursor.fetchall()

# Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['O_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY'])
    for row in results:
        csvwriter.writerow(row)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
