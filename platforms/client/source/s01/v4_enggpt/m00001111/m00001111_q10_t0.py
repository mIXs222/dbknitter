import pymysql
import pymongo
from decimal import Decimal
import csv
from datetime import datetime

# Define function to connect to MySQL
def get_mysql_connection():
    connection = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 db='tpch')
    return connection

# Define function to connect to MongoDB
def get_mongo_client():
    client = pymongo.MongoClient('mongodb', 27017)
    return client

# Start of the main script
mysql_conn = get_mysql_connection()
mongo_client = get_mongo_client()
mongo_db = mongo_client.tpch

# Fetch nation data from MySQL
nations = {}
with mysql_conn.cursor() as cursor:
    query = "SELECT N_NATIONKEY, N_NAME FROM nation"
    cursor.execute(query)
    for record in cursor.fetchall():
        nations[record[0]] = record[1]

# Fetch customer data from MongoDB
customers = {c['C_CUSTKEY']: c for c in mongo_db.customer.find({}, {'_id': 0})}

# Fetch orders data from MongoDB
start_date = datetime.strptime('1993-10-01', '%Y-%m-%d')
end_date = datetime.strptime('1993-12-31', '%Y-%m-%d')
orders = mongo_db.orders.find({'O_ORDERDATE': {'$gte': start_date, '$lte': end_date}}, {'_id': 0})

# Fetch lineitem data and calculate revenue
customers_revenue = {}
for order in orders:
    lineitems = mongo_db.lineitem.find({'L_ORDERKEY': order['O_ORDERKEY'], 'L_RETURNFLAG': 'R'}, {'_id': 0})
    for lineitem in lineitems:
        revenue = (lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT']))
        custkey = order['O_CUSTKEY']
        if custkey not in customers_revenue:
            customers_revenue[custkey] = Decimal('0.00')
        customers_revenue[custkey] += revenue

# Compile final results
results = []
for custkey, revenue in customers_revenue.items():
    if custkey in customers:
        customer = customers[custkey]
        result = {
            'C_CUSTKEY': custkey,
            'C_NAME': customer['C_NAME'],
            'REVENUE': revenue.quantize(Decimal('0.00')),
            'C_ACCTBAL': customer['C_ACCTBAL'],
            'N_NAME': nations.get(customer['C_NATIONKEY'], ''),
            'C_ADDRESS': customer['C_ADDRESS'],
            'C_PHONE': customer['C_PHONE'],
            'C_COMMENT': customer['C_COMMENT']
        }
        results.append(result)

# Sort results based on specification
sorted_results = sorted(results, key=lambda x: (x['REVENUE'], x['C_CUSTKEY'], x['C_NAME'], -x['C_ACCTBAL']))

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['C_CUSTKEY', 'C_NAME', 'REVENUE', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for row in sorted_results:
        writer.writerow(row)

# Close all connections
mysql_conn.close()
mongo_client.close()
