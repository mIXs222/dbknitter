import pymysql
import pymongo
import csv

# Establish connection to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Establish connection to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
customer_col = mongodb['customer']

# Retrieve customer data from MongoDB where C_MKTSEGMENT is 'BUILDING'
customer_data = list(customer_col.find({'C_MKTSEGMENT': 'BUILDING'}, {'_id': 0, 'C_CUSTKEY': 1}))

# If no customer data is retrieved, we can close the connections as no further query is needed
if not customer_data:
    mysql_cursor.close()
    mysql_conn.close()
    mongo_client.close()
    with open('query_output.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['L_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY'])
    exit()

# Extract the C_CUSTKEYs from the customer data
custkeys = [cust['C_CUSTKEY'] for cust in customer_data]

# Perform the SQL query using only the custkeys retrieved from MongoDB
sql_query = '''
SELECT
    L_ORDERKEY,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE,
    O_ORDERDATE,
    O_SHIPPRIORITY
FROM
    orders,
    lineitem
WHERE
    O_CUSTKEY IN (%s)
    AND L_ORDERKEY = O_ORDERKEY
    AND O_ORDERDATE < '1995-03-15'
    AND L_SHIPDATE > '1995-03-15'
GROUP BY
    L_ORDERKEY,
    O_ORDERDATE,
    O_SHIPPRIORITY
ORDER BY
    REVENUE DESC,
    O_ORDERDATE
''' % ','.join(map(str, custkeys))  # We create a list of custkeys as a string

# Execute the query
mysql_cursor.execute(sql_query)

# Grab the result
result = mysql_cursor.fetchall()

# Close the MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Close the MongoDB connection
mongo_client.close()

# Write result to the CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['L_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY'])
    writer.writerows(result)
