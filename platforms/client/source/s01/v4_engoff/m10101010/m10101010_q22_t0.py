# Python Code: execute_query.py

import pymysql
import pymongo
import csv
from datetime import datetime, timedelta

# Connection details for MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connection details for MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]

# Prepare the output file
output_file = 'query_output.csv'

# Get the current date and calculate the date 7 years ago
current_date = datetime.now()
seven_years_ago = current_date - timedelta(days=7*365)

# Define the list of valid country codes
valid_codes = ('20', '40', '22', '30', '39', '42', '21')

try:
    with mysql_conn.cursor() as mysql_cursor:
        # Query to select customers from MySQL whose phone starts with the valid country codes
        mysql_cursor.execute("""
            SELECT C_NATIONKEY, COUNT(*) AS customer_count, AVG(C_ACCTBAL) AS average_balance
            FROM customer
            WHERE C_PHONE LIKE '20%%' OR C_PHONE LIKE '40%%' OR
                  C_PHONE LIKE '22%%' OR C_PHONE LIKE '30%%' OR
                  C_PHONE LIKE '39%%' OR C_PHONE LIKE '42%%' OR
                  C_PHONE LIKE '21%%' AND
                  C_ACCTBAL > 0
            GROUP BY C_NATIONKEY
        """)
        mysql_result = {row['C_NATIONKEY']: {'customer_count': row['customer_count'], 'average_balance': row['average_balance']} for row in mysql_cursor.fetchall()}

    # Query to select orders from MongoDB whose order date is less than 7 years ago
    mongo_collection = mongo_db['orders']
    mongo_result = mongo_collection.find(
        {
            'O_ORDERDATE': {'$lt': seven_years_ago.isoformat()}
        },
        {
            'O_CUSTKEY': 1
        }
    )
    # Convert the MongoDB result into a set of customer keys
    customers_with_recent_orders = set(doc['O_CUSTKEY'] for doc in mongo_result)

    # Open the output file for writing
    with open(output_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        # Write header
        writer.writerow(['C_NATIONKEY', 'CUSTOMER_COUNT', 'AVERAGE_BALANCE'])

        # Iterate over MySQL results and write rows for customers without recent orders
        for nation_key, data in mysql_result.items():
            if nation_key not in customers_with_recent_orders:
                writer.writerow([nation_key, data['customer_count'], data['average_balance']])

finally:
    mysql_conn.close()
    mongo_client.close()
