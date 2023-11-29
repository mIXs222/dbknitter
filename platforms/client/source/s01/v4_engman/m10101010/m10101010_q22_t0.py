import pymysql
import pymongo
import csv
from datetime import datetime, timedelta

# Function to get mysql connection
def get_mysql_connection():
    return pymysql.connect(
        host='mysql',
        user='root',
        passwd='my-secret-pw',
        db='tpch'
    )

# Function to get mongodb connection
def get_mongodb_connection():
    client = pymongo.MongoClient('mongodb', 27017)
    return client['tpch']

# Connect to MySQL
mysql_conn = get_mysql_connection()
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongodb_conn = get_mongodb_connection()
orders_collection = mongodb_conn['orders']

# Current date to calculate 7 years ago
current_date = datetime.now()
seven_years_ago = current_date - timedelta(days=7*365)

# Get average account balance from MySQL
average_balance_query = """
    SELECT C_NATIONKEY, AVG(C_ACCTBAL)
    FROM customer
    WHERE C_ACCTBAL > 0.00
    GROUP BY C_NATIONKEY
"""
mysql_cursor.execute(average_balance_query)
average_balance_result = {row[0]: row[1] for row in mysql_cursor}

# Get all customer records from MySQL with specific C_PHONE country codes
customer_query = """
    SELECT C_CUSTKEY, C_ACCTBAL, SUBSTRING(C_PHONE, 1, 2) AS CNTRYCODE
    FROM customer
    WHERE SUBSTRING(C_PHONE, 1, 2) IN ('20', '40', '22', '30', '39', '42', '21')
"""
mysql_cursor.execute(customer_query)
customer_data = mysql_cursor.fetchall()

# Get all customer keys from MongoDB who have placed orders in the last 7 years
order_customer_keys = set(
    doc['O_CUSTKEY']
    for doc in orders_collection.find({'O_ORDERDATE': {'$gte': seven_years_ago.isoformat()}})
)

# Prepare the output data
output_data = {}
for custkey, acctbal, cntrycode in customer_data:
    if custkey not in order_customer_keys and acctbal > average_balance_result.get(int(cntrycode), 0):
        if cntrycode not in output_data:
            output_data[cntrycode] = {'count': 0, 'total_balance': 0}
        output_data[cntrycode]['count'] += 1
        output_data[cntrycode]['total_balance'] += acctbal

# Write output to csv file
output_filename = 'query_output.csv'
with open(output_filename, 'w', newline='') as csvfile:
    fieldnames = ['CNTRYCODE', 'CUSTOMER_COUNT', 'TOTAL_BALANCE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    writer.writeheader()
    for cntrycode in sorted(output_data):
        writer.writerow({
            'CNTRYCODE': cntrycode,
            'CUSTOMER_COUNT': output_data[cntrycode]['count'],
            'TOTAL_BALANCE': output_data[cntrycode]['total_balance']
        })

# Close the connections
mysql_cursor.close()
mysql_conn.close()
mongodb_conn.close()
