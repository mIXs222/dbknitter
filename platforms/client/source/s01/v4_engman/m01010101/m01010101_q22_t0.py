# query.py
import pymysql
import pymongo
import csv
from datetime import datetime, timedelta

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Get customers who haven't ordered for 7 years
seven_years_ago = (datetime.today() - timedelta(days=7*365)).date()
with mysql_conn.cursor() as mysql_cursor:
    mysql_cursor.execute("""
    SELECT O_CUSTKEY
    FROM orders
    WHERE O_ORDERDATE >= %s
    """, (seven_years_ago,))
    recent_customers = {row[0] for row in mysql_cursor.fetchall()}

# Calculate average account balance for customers with C_ACCTBAL > 0
avg_acct_balance = mongodb.customer.aggregate([
    {'$match': {'C_ACCTBAL': {'$gt': 0}}},
    {'$group': {'_id': None, 'avg_balance': {'$avg': '$C_ACCTBAL'}}}
]).next()['avg_balance']

# Get customers by country code with account balance greater than average
country_codes = ['20', '40', '22', '30', '39', '42', '21']
results = []
for code in country_codes:
    customers = list(mongodb.customer.find(
        {
            'C_PHONE': {'$regex': f'^{code}'},
            'C_CUSTKEY': {'$nin': list(recent_customers)},
            'C_ACCTBAL': {'$gt': avg_acct_balance}
        },
        {'_id': 0, 'C_ACCTBAL': 1}
    ))
    num_customers = len(customers)
    total_acct_balance = sum(customer['C_ACCTBAL'] for customer in customers)
    results.append((code, num_customers, total_acct_balance))

# Sort results by country code
results.sort(key=lambda x: x[0])

# Output to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['CNTRYCODE', 'NUM_CUSTOMERS', 'TOTAL_ACCT_BALANCE'])
    for result in results:
        csvwriter.writerow(result)

# Close connections
mysql_conn.close()
mongo_client.close()
