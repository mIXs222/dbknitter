import csv
import pymysql
import pymongo
from datetime import datetime, timedelta

# MySQL Connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# MongoDB Connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Process customers in MongoDB and gather relevant data
country_codes = ['20', '40', '22', '30', '39', '42', '21']
seven_years_ago = datetime.utcnow() - timedelta(days=7*365)
mongo_customers = mongodb['customer'].find(
    {"C_PHONE": {"$regex": "^(?:" + "|".join(country_codes) + ")"},
     "C_ACCTBAL": {"$gt": 0}}
)
average_balance = {}
customer_data = {}

for cust in mongo_customers:
    country_code = cust['C_PHONE'][:2]
    customer_key = cust['C_CUSTKEY']
    if country_code not in average_balance:
        average_balance[country_code] = {'total': 0, 'count': 0}
        customer_data[country_code] = {'count': 0, 'total_balance': 0}

    average_balance[country_code]['total'] += cust['C_ACCTBAL']
    average_balance[country_code]['count'] += 1

    customer_data[country_code]['count'] += 1
    customer_data[country_code]['total_balance'] += cust['C_ACCTBAL']

# Get the average balances greater than 0 and exclude customers with orders in the last 7 years from MySQL
with mysql_conn.cursor() as cursor:
    for cc in country_codes:
        if cc in average_balance and average_balance[cc]['count'] > 0:
            avg_bal = average_balance[cc]['total'] / average_balance[cc]['count']
            cursor.execute(
                "SELECT o.O_CUSTKEY FROM orders o WHERE o.O_ORDERDATE >= %s AND o.O_CUSTKEY in ("
                "SELECT c.C_CUSTKEY FROM customer c WHERE c.C_PHONE LIKE %s)",
                (seven_years_ago, cc + '%')
            )
            active_customers = cursor.fetchall()
            active_customer_keys = {row[0] for row in active_customers}
            
            # Exclude active customers
            for active_cust_key in active_customer_keys:
                if active_cust_key in customer_data[cc]['count']:
                    customer_data[cc]['total_balance'] -= active_cust_key
                    customer_data[cc]['count'] -= 1

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['CNTRYCODE', 'CUSTOMER_COUNT', 'TOTAL_BALANCE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for code in sorted(customer_data.keys()):
        row = {
            'CNTRYCODE': code,
            'CUSTOMER_COUNT': customer_data[code]['count'],
            'TOTAL_BALANCE': customer_data[code]['total_balance']
        }
        writer.writerow(row)

# Close connections
mysql_conn.close()
mongo_client.close()
