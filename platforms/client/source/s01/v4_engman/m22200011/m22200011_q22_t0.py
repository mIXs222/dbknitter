import pymysql
import pymongo
import csv
from datetime import datetime, timedelta

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# MongoDB connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Get current year and date 7 years ago
current_year = datetime.now().year
date_seven_years_ago = datetime.now() - timedelta(days=7*365)

# Define country codes
country_codes = ('20', '40', '22', '30', '39', '42', '21')

# Fetch customers from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT C_PHONE, COUNT(*), SUM(C_ACCTBAL) 
        FROM customer 
        WHERE 
            C_PHONE LIKE %s AND 
            C_ACCTBAL > (
                SELECT AVG(C_ACCTBAL) 
                FROM customer 
                WHERE C_ACCTBAL > 0.00 AND 
                C_PHONE LIKE %s
            )
        GROUP BY C_PHONE
        """, [code + '%', code + '%'] for code in country_codes)
    
    mysql_customers = cursor.fetchall()

# Fetch orders from MongoDB
orders = mongodb_db.orders.find({
    "O_ORDERDATE": {"$lt": date_seven_years_ago}
})

# List of customers who have not placed orders for 7 years
no_orders_customers = []

for order in orders:
    no_orders_customers.append(order['O_CUSTKEY'])

# Filter out customers who have not placed orders for 7 years
filtered_customers = list(filter(lambda x: x[0] in country_codes and x[1] not in no_orders_customers, mysql_customers))

# Prepare data for CSV
data_for_csv = []

for customer in filtered_customers:
    data_for_csv.append([
        customer[0][:2],  # CNTRYCODE
        customer[1],      # Number of customers
        customer[2]       # Total account balance
    ])

data_for_csv = sorted(data_for_csv, key=lambda x: x[0])  # Order by CNTRYCODE ascending

# Write query output to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['CNTRYCODE', 'NumOfCustomers', 'TotalAcctBal'])
    csvwriter.writerows(data_for_csv)

# Close connections
mysql_conn.close()
mongodb_client.close()
