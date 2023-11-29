import pymysql
import pymongo
import csv
from datetime import datetime, timedelta
from decimal import Decimal

# Constants for the country codes
ALLOWED_COUNTRY_CODES = ['20', '40', '22', '30', '39', '42', '21']

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
try:
    with mysql_conn.cursor() as cursor:
        # Get average account balance greater than 0 for given country codes
        country_code_balances = {}
        for code in ALLOWED_COUNTRY_CODES:
            query = """
                SELECT AVG(C_ACCTBAL) 
                FROM customer 
                WHERE C_ACCTBAL > 0.00 AND SUBSTRING(C_PHONE, 1, 2) = %s
            """
            cursor.execute(query, (code,))
            avg_balance = cursor.fetchone()[0] or 0
            country_code_balances[code] = avg_balance

        # Define 7 years ago
        seven_years_ago = datetime.now() - timedelta(days=7*365)

        # Prepare MongoDB connection
        mongodb_client = pymongo.MongoClient("mongodb://mongodb:27017/")
        mongodb_db = mongodb_client["tpch"]
        customer_collection = mongodb_db["customer"]

        output_rows = []

        # Query MongoDB for customers per country code with conditions
        for code, avg_balance in country_code_balances.items():
            customers = customer_collection.find({
                'C_PHONE': {'$regex': f'^{code}'},
                'C_ACCTBAL': {'$gt': avg_balance}
            })
            num_customers = 0
            total_balance = Decimal('0.00')
            for customer in customers:
                # Check if the customer has not made orders within 7 years
                last_order = list(cursor.execute(
                    "SELECT MAX(O_ORDERDATE) FROM orders WHERE O_CUSTKEY = %s",
                    (customer['C_CUSTKEY'],)
                ))
                if len(last_order) == 0 or last_order[0][0] < seven_years_ago:
                    num_customers += 1
                    total_balance += Decimal(str(customer['C_ACCTBAL']))

            output_rows.append([code, num_customers, str(total_balance)])
finally:
    mysql_conn.close()

# Sort output rows by country code ascending
output_rows.sort(key=lambda x: x[0])

# Write the output rows to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['CNTRYCODE', 'NUM_CUSTOMERS', 'TOTAL_BALANCE'])
    for row in output_rows:
        csvwriter.writerow(row)
