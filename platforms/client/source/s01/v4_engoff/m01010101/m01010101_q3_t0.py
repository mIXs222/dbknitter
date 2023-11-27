import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
customers = mongo_db['customer']

# Get customer keys for market segment 'BUILDING'
building_customers = customers.find({'C_MKTSEGMENT': 'BUILDING'}, {'C_CUSTKEY': 1, '_id': 0})

# Extract customer keys into a list
building_customer_keys = [customer['C_CUSTKEY'] for customer in building_customers]

# Query the orders from MySQL
mysql_query = """
SELECT
    O_ORDERKEY,
    O_SHIPPRIORITY,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS revenue
FROM
    orders
JOIN lineitem ON O_ORDERKEY = L_ORDERKEY
WHERE
    O_ORDERDATE < '1995-03-15'
    AND O_ORDERSTATUS = 'O'
    AND O_CUSTKEY IN (%s)
GROUP BY O_ORDERKEY
ORDER BY revenue DESC
"""
# Formatting the query with customer keys (safe as these come from the db itself)
formatted_query = mysql_query % ','.join(map(str, building_customer_keys))
mysql_cursor.execute(formatted_query)

# Fetch the results
results = mysql_cursor.fetchall()

# Write the output to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['O_ORDERKEY', 'O_SHIPPRIORITY', 'REVENUE'])
    for row in results:
        csvwriter.writerow(row)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
