import pymysql
import pymongo
import csv
import datetime

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   database='tpch')
mysql_cursor = mysql_connection.cursor()

# Connect to MongoDB
mongodb_client = pymongo.MongoClient(host='mongodb', port=27017)
mongodb_db = mongodb_client['tpch']
mongodb_orders_collection = mongodb_db['orders']

# Calculate the cutoff date, 7 years back
cutoff_date = datetime.datetime.now() - datetime.timedelta(days=7*365)

# Query MySQL database for customers
country_codes = ('20', '40', '22', '30', '39', '42', '21')
mysql_cursor.execute("""
SELECT LEFT(C_PHONE, 2) AS COUNTRY_CODE, COUNT(*) AS CUSTOMER_COUNT, AVG(C_ACCTBAL) AS AVG_ACCTBAL
FROM customer
WHERE LEFT(C_PHONE, 2) IN %s AND C_ACCTBAL > 0.00
GROUP BY LEFT(C_PHONE, 2)
""", (country_codes,))

# Get customers who have positive account balance and match country codes
customers_result = mysql_cursor.fetchall()

# Get customer IDs who have not placed orders for 7 years
customers_no_orders = []
for customer in customers_result:
    COUNTRY_CODE = customer[0]
    cust_ids = mongodb_orders_collection.find({
        "O_ORDERDATE": {"$lt": cutoff_date},
        "O_CUSTKEY": {"$regex": f'^{COUNTRY_CODE}'}
    }, {"O_CUSTKEY": 1})
    cust_ids = set(doc['O_CUSTKEY'] for doc in cust_ids)  # Create set of customer IDs

    customers_no_orders.append((COUNTRY_CODE, customer[1] - len(cust_ids), customer[2]))

# Write output to CSV
with open('query_output.csv', 'w', newline='', encoding='utf-8') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(['COUNTRY_CODE', 'CUSTOMER_COUNT', 'AVG_ACCTBAL'])
    for row in customers_no_orders:
        csv_writer.writerow(row)

# Close connections
mysql_cursor.close()
mysql_connection.close()
mongodb_client.close()
