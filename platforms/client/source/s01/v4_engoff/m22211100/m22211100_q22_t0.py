import pymysql
import pymongo
import csv
import datetime

# Constants
MYSQL_HOST = 'mysql'
MYSQL_DB = 'tpch'
MYSQL_USER = 'root'
MYSQL_PASS = 'my-secret-pw'
MONGO_HOST = 'mongodb'
MONGO_PORT = 27017
MONGO_DB = 'tpch'
VALID_COUNTRY_PREFIXES = ['20', '40', '22', '30', '39', '42', '21']
SEVEN_YEARS_AGO = datetime.datetime.now() - datetime.timedelta(days=7*365)

# Connect to MySQL
mysql_conn = pymysql.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASS, db=MYSQL_DB)
mysql_cursor = mysql_conn.cursor()

# Fetch customers with no orders in the last 7 years
mysql_cursor.execute(
    'SELECT O_CUSTKEY FROM orders WHERE O_ORDERDATE < %s', 
    (SEVEN_YEARS_AGO,)
)
no_orders_customers = set(row[0] for row in mysql_cursor.fetchall())

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Function to check if phone country code is valid
def valid_country_code(phone):
    return phone[:2] in VALID_COUNTRY_PREFIXES

# Connect to MongoDB
mongo_client = pymongo.MongoClient(host=MONGO_HOST, port=MONGO_PORT)
mongo_db = mongo_client[MONGO_DB]

# Process MongoDB customers
customers = mongo_db.customer.find()
result = []

for customer in customers:
    if customer['C_CUSTKEY'] not in no_orders_customers and valid_country_code(customer['C_PHONE']) and customer['C_ACCTBAL'] > 0.00:
        result.append({
            'C_CUSTKEY': customer['C_CUSTKEY'],
            'C_COUNTRY_CODE': customer['C_PHONE'][:2],
            'C_ACCTBAL': customer['C_ACCTBAL']
        })

# Write to CSV file
with open('query_output.csv', mode='w', newline='') as file:
    fieldnames = ['C_CUSTKEY', 'C_COUNTRY_CODE', 'C_ACCTBAL']
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    
    writer.writeheader()
    for data in result:
        writer.writerow(data)

# Close MongoDB connection
mongo_client.close()
