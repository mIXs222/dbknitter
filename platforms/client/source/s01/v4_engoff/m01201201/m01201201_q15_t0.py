import pymysql
import pymongo
import csv
import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
)
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
lineitem_collection = mongo_db['lineitem']

# Get suppliers from MySQL
suppliers = {}
query = "SELECT * FROM supplier"
mysql_cursor.execute(query)
for s in mysql_cursor.fetchall():
    suppliers[s[0]] = {'S_NAME': s[1], 'revenue': 0}

# Process lineitems from MongoDB
start_date = datetime.datetime(1996, 1, 1)
end_date = datetime.datetime(1996, 4, 1)
lineitems = lineitem_collection.find({
    'L_SHIPDATE': {'$gte': start_date, '$lt': end_date}
})

# Calculate revenue for each supplier
for lineitem in lineitems:
    suppkey = lineitem['L_SUPPKEY']
    revenue = lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])
    if suppkey in suppliers:
        suppliers[suppkey]['revenue'] += revenue

# Determine top supplier(s)
max_revenue = max(s['revenue'] for s in suppliers.values())

# Collect top suppliers
top_suppliers = [
    (suppkey, s['S_NAME'], s['revenue'])
    for suppkey, s in suppliers.items()
    if s['revenue'] == max_revenue
]

# Sort by supplier key
top_suppliers.sort()

# Write to CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['S_SUPPKEY', 'S_NAME', 'REVENUE'])
    for supplier in top_suppliers:
        writer.writerow(supplier)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
