# import necessary libraries
import csv
import pymysql
import pymongo
from datetime import datetime

# Constants for MySQL connection
MYSQL_DB_NAME = "tpch"
MYSQL_USERNAME = "root"
MYSQL_PASSWORD = "my-secret-pw"
MYSQL_HOSTNAME = "mysql"

# Constants for MongoDB connection
MONGO_DB_NAME = "tpch"
MONGO_PORT = 27017
MONGO_HOSTNAME = "mongodb"

# Function to fetch lineitem data from MySQL
def fetch_lineitem_data(conn):
    start_date = datetime(1996, 1, 1)
    end_date = datetime(1996, 3, 31)
    with conn.cursor() as cursor:
        query = """
        SELECT 
            L_SUPPKEY AS SUPPLIER_NO,
            SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS TOTAL_REVENUE
        FROM 
            lineitem
        WHERE 
            L_SHIPDATE BETWEEN %s AND %s
        GROUP BY 
            L_SUPPKEY
        """
        cursor.execute(query, (start_date, end_date))
        return cursor.fetchall()

# Function to fetch supplier data from MongoDB
def fetch_supplier_data(mongo_db):
    supplier_collection = mongo_db.supplier
    return list(supplier_collection.find({}))

# Connect to MySQL
mysql_conn = pymysql.connect(host=MYSQL_HOSTNAME, user=MYSQL_USERNAME, password=MYSQL_PASSWORD, db=MYSQL_DB_NAME)

# Connect to MongoDB
mongo_client = pymongo.MongoClient(MONGO_HOSTNAME, MONGO_PORT)
mongo_db = mongo_client[MONGO_DB_NAME]

# Fetch data from MySQL and MongoDB
lineitem_data = fetch_lineitem_data(mysql_conn)
supplier_data = fetch_supplier_data(mongo_db)

# Creating a dictionary of supplier total revenue
supplier_revenue = {row['SUPPLIER_NO']: row['TOTAL_REVENUE'] for row in lineitem_data}

# Finding the supplier with the maximum revenue
max_revenue = max(supplier_revenue.values())
max_revenue_supplier = [key for key, value in supplier_revenue.items() if value == max_revenue]

# Creating a list of suppliers with max revenue and their details
max_revenue_supplier_details = []
for supplier in supplier_data:
    if supplier['S_SUPPKEY'] in max_revenue_supplier:
        max_revenue_supplier_details.append({
            'S_SUPPKEY': supplier['S_SUPPKEY'],
            'S_NAME': supplier['S_NAME'],
            'S_ADDRESS': supplier['S_ADDRESS'],
            'S_PHONE': supplier['S_PHONE'],
            'TOTAL_REVENUE': supplier_revenue[supplier['S_SUPPKEY']]
        })

# Sort by S_SUPPKEY
max_revenue_supplier_details_sorted = sorted(max_revenue_supplier_details, key=lambda k: k['S_SUPPKEY'])

# Closing connections
mysql_conn.close()
mongo_client.close()

# Write the results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for row in max_revenue_supplier_details_sorted:
        writer.writerow(row)
