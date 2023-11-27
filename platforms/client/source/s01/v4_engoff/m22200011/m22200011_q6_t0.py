from pymongo import MongoClient
import csv
import datetime

# Function to connect to the MongoDB database
def connect_to_mongodb(host, port, db_name):
    client = MongoClient(host, port)
    db = client[db_name]
    return db

# MongoDB connection details
mongodb_host = 'mongodb'
mongodb_port = 27017
mongodb_db_name = 'tpch'

# Connect to the MongoDB database
db = connect_to_mongodb(mongodb_host, mongodb_port, mongodb_db_name)

# Define the query criteria
start_date = datetime.datetime(1994, 1, 1)
end_date = datetime.datetime(1995, 1, 1)
min_discount = 0.06 - 0.01
max_discount = 0.06 + 0.01
max_quantity = 24

# Perform the query on the 'lineitem' collection
lineitems = db.lineitem.find({
    'L_SHIPDATE': {'$gte': start_date, '$lt': end_date},
    'L_DISCOUNT': {'$gte': min_discount, '$lte': max_discount},
    'L_QUANTITY': {'$lt': max_quantity}
})

# Calculate the potential revenue increase
total_revenue_increase = sum(
    item['L_EXTENDEDPRICE'] * item['L_DISCOUNT'] for item in lineitems
)

# Output the result to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    # Write header
    writer.writerow(['Total Revenue Increase'])
    # Write data
    writer.writerow([total_revenue_increase])

print('Query completed. Results saved to query_output.csv.')
