import csv
import pymysql
import pymongo

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client['tpch']
mongo_customers = mongo_db['customer']

# Get all customers from MongoDB
customers = list(mongo_customers.find({}, {"_id": 0, "C_CUSTKEY": 1}))

# Convert customers list of dicts to dict for faster access
customers_dict = {customer['C_CUSTKEY']: 0 for customer in customers}

# Prepare and execute query on MySQL
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute("SELECT O_CUSTKEY, COUNT(O_ORDERKEY) FROM orders WHERE O_COMMENT NOT LIKE '%pending%deposits%' GROUP BY O_CUSTKEY")

# Tally orders count with customers
for row in mysql_cursor.fetchall():
    c_custkey, count = row
    if c_custkey in customers_dict:
        customers_dict[c_custkey] = count

# Aggregate counts
counts_dict = {}
for count in customers_dict.values():
    counts_dict.setdefault(count, 0)
    counts_dict[count] += 1

# Sort and write results to CSV file
with open('query_output.csv', mode='w', newline='') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(['C_COUNT', 'CUSTDIST'])
    for count, custdist in sorted(counts_dict.items(), key=lambda x: (-x[1], -x[0])):
        writer.writerow([count, custdist])

# Close all connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
