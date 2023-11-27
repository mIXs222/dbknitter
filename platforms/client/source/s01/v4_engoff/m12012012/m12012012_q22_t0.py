import pymysql
import pymongo
import csv
import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
orders_collection = mongo_db["orders"]

# Find all customers who have not placed orders for 7 years
seven_years_ago = datetime.datetime.now() - datetime.timedelta(days=7*365)
ancient_orders = list(orders_collection.find({
    "O_ORDERDATE": {"$lt": seven_years_ago}
}, {"O_CUSTKEY": 1}))

ancient_custkeys = {order["O_CUSTKEY"] for order in ancient_orders}

# Construct the SQL query for MySQL
country_codes = ['20', '40', '22', '30', '39', '42', '21']
placeholders = ', '.join(['%s'] * len(country_codes))
query = f"""
SELECT LEFT(C_PHONE, 2) AS country_code, COUNT(*) AS num_customers, AVG(C_ACCTBAL) AS avg_balance
FROM customer
WHERE LEFT(C_PHONE, 2) IN ({placeholders})
AND C_ACCTBAL > 0
AND C_CUSTKEY NOT IN ({', '.join(['%s']*len(ancient_custkeys))})
GROUP BY LEFT(C_PHONE, 2)
"""

# Execute the query
mysql_cursor.execute(query, country_codes + list(ancient_custkeys))
results = mysql_cursor.fetchall()

# Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['country_code', 'num_customers', 'avg_balance'])
    for row in results:
        csvwriter.writerow(row)

# Close the connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
