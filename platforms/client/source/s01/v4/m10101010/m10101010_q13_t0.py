# execute_query.py
import pymysql
import pymongo
import csv

# Establishing the MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Establishing the MongoDB connection
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongodb = mongo_client['tpch']

# Fetching data from MySQL
with mysql_conn:
    with mysql_conn.cursor() as cursor:
        cursor.execute("SELECT C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT FROM customer")
        customers = cursor.fetchall()

# Fetching data from MongoDB
orders = list(mongodb.orders.find({}, {'_id': 0}))

# Processing data and performing join in Python
def filter_orders(orders, customer_key):
    count = 0
    for order in orders:
        if order['O_CUSTKEY'] == customer_key and 'pending deposits' not in order['O_COMMENT']:
            count += 1
    return count

cust_with_count = [(cust[0], filter_orders(orders, cust[0])) for cust in customers]
cust_count_dict = {}
for cust in cust_with_count:
    cust_count_dict.setdefault(cust[1], 0)
    cust_count_dict[cust[1]] += 1

# Sorting based on the requirements
sorted_cust_count = sorted(cust_count_dict.items(), key=lambda x: (-x[1], -x[0]))

# Writing to query_output.csv
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['C_COUNT', 'CUSTDIST'])
    for row in sorted_cust_count:
        writer.writerow(row)

# Closing connections
mysql_conn.close()
mongo_client.close()
