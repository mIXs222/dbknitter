import pymysql
import pymongo
import csv
from datetime import datetime, timedelta

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# MongoDB connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# MySQL: Get customers who have not placed orders for 7 years
seven_years_ago = datetime.now() - timedelta(days=7 * 365)
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute("""
SELECT O_CUSTKEY FROM orders WHERE O_ORDERDATE < %s
""", (seven_years_ago,))
inactive_customer_ids = set(row[0] for row in mysql_cursor.fetchall())
mysql_cursor.close()

# MongoDB: Get customers with positive account balance and matching country code
country_codes = ['20', '40', '22', '30', '39', '42', '21']
customers = mongodb_db.customer.find({
    'C_ACCTBAL': {'$gt': 0.00},
    'C_PHONE': {'$regex': f'^({"|".join(country_codes)})'},
})

# Compile results and write to CSV
with open('query_output.csv', mode='w') as file:
    writer = csv.writer(file)
    writer.writerow(['Country_Code', 'Customer_Count', 'Total_Balance'])

    results = {}
    for customer in customers:
        if customer['C_CUSTKEY'] not in inactive_customer_ids:
            country_code = customer['C_PHONE'][:2]
            if country_code in country_codes:
                if country_code not in results:
                    results[country_code] = {'count': 0, 'balance': 0.00}
                results[country_code]['count'] += 1
                results[country_code]['balance'] += customer['C_ACCTBAL']

    for country_code, data in results.items():
        writer.writerow([country_code, data['count'], data['balance']])

# Close connections
mysql_conn.close()
mongodb_client.close()
