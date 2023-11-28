import pymongo
import pymysql
import csv
from datetime import datetime

# Connection to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Connection to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# MySQL query
mysql_query = """
SELECT c.C_CUSTKEY, c.C_NAME, SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS REVENUE, c.C_ACCTBAL,
       c.C_ADDRESS, c.C_PHONE, c.C_COMMENT
FROM customer AS c
JOIN lineitem AS l ON c.C_CUSTKEY = l.L_ORDERKEY
WHERE l.L_RETURNFLAG = 'R'
GROUP BY c.C_CUSTKEY
"""

# MongoDB query
mongodb_query = {
    'O_ORDERDATE': {
        '$gte': datetime(1993, 10, 1),
        '$lte': datetime(1993, 12, 31)
    }
}

customers_revenue = {}
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    for row in cursor.fetchall():
        custkey, name, revenue, acctbal, address, phone, comment = row
        customers_revenue[custkey] = {
            'C_NAME': name,
            'REVENUE': float(revenue) if revenue else 0,
            'C_ACCTBAL': acctbal,
            'C_ADDRESS': address,
            'C_PHONE': phone,
            'C_COMMENT': comment
        }

orders = mongodb_db.orders.find(mongodb_query, {'O_CUSTKEY': 1})
nation_dict = {doc['N_NATIONKEY']: doc['N_NAME'] for doc in mongodb_db.nation.find()}

for order in orders:
    if order['O_CUSTKEY'] in customers_revenue:
        customers_revenue[order['O_CUSTKEY']]['N_NAME'] = nation_dict.get(order['O_CUSTKEY'], '')

output_data = list(customers_revenue.values())
output_data.sort(key=lambda x: (x['REVENUE'], x['C_CUSTKEY'], x['C_NAME'], -x['C_ACCTBAL']))

# Write to CSV file
csv_headers = ['C_CUSTKEY', 'C_NAME', 'REVENUE', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=csv_headers)
    writer.writeheader()
    for data in output_data:
        writer.writerow(data)

# Close connections
mysql_conn.close()
mongodb_client.close()
