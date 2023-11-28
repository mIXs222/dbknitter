import pymysql
import pymongo
import csv

# MySQL Connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)
mysql_cursor = mysql_conn.cursor()

# MongoDB Connection
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']
mongo_customers = mongo_db['customer']

# Fetch all customers from MongoDB
all_customers = list(mongo_customers.find({}, {'_id': 0, 'C_CUSTKEY': 1}))

# Create a dictionary with customer keys and initialize their order count to 0
customer_orders = {customer['C_CUSTKEY']: 0 for customer in all_customers}

# Query to select orders from MySQL excluding the ones with 'pending' and 'deposits' in the comments
order_query = """
SELECT O_CUSTKEY, COUNT(*) AS C_COUNT
FROM orders
WHERE O_COMMENT NOT LIKE '%pending%' AND O_COMMENT NOT LIKE '%deposits%'
GROUP BY O_CUSTKEY;
"""

# Execute the query
mysql_cursor.execute(order_query)
order_results = mysql_cursor.fetchall()

# Update customer order counts
for order in order_results:
    custkey, count = order
    if custkey in customer_orders:
        customer_orders[custkey] = count

# Generating the final distribution
custdist = {}
for count in customer_orders.values():
    custdist[count] = custdist.get(count, 0) + 1

# Sorting the final distribution
sorted_custdist = sorted(custdist.items(), key=lambda x: (-x[1], -x[0]))

# Writing results to CSV
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['C_COUNT', 'CUSTDIST'])
    for count, dist in sorted_custdist:
        writer.writerow([count, dist])

# Closing connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
