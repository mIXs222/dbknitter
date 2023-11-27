import pymysql
import pymongo
import csv
from datetime import datetime, timedelta

# Connection to the MySQL database
mysql_conn = pymysql.connect(
    host='mysql', 
    user='root', 
    password='my-secret-pw', 
    database='tpch'
)

# Connection to the MongoDB database
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']

# Function to get the customer data from MongoDB
def get_customers():
    country_codes = ['20', '40', '22', '30', '39', '42', '21']
    seven_years_ago = datetime.now() - timedelta(days=7*365)
    customer_data = mongo_db['customer'].find({
        'C_PHONE': {'$regex': '^(20|40|22|30|39|42|21)'},
        'C_ACCTBAL': {'$gt': 0}
    })
    return {
        cust['C_CUSTKEY']: {
            'C_NAME': cust['C_NAME'],
            'C_ACCTBAL': cust['C_ACCTBAL'],
            'C_PHONE': cust['C_PHONE']
        }
        for cust in customer_data
    }

# Function to query orders from MySQL
def get_orders_for_customers(customer_keys):
    with mysql_conn.cursor() as cursor:
        format_strings = ','.join(['%s'] * len(customer_keys))
        customer_keys_tuple = tuple(customer_keys)
        cursor.execute(f"""
            SELECT O_CUSTKEY 
            FROM orders 
            WHERE O_CUSTKEY IN ({format_strings}) 
            AND O_ORDERDATE >= %s
        """, customer_keys_tuple + (seven_years_ago,))
        return {row[0] for row in cursor.fetchall()}

# Process the data and output to CSV
customers = get_customers()
orders = get_orders_for_customers(customers.keys())
filtered_customers = {
    k: v for k, v in customers.items() if k not in orders
}

with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['C_NAME', 'C_ACCTBAL', 'C_PHONE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    writer.writeheader()
    for customer in filtered_customers.values():
        writer.writerow(customer)

# Close connections
mysql_conn.close()
mongo_client.close()
