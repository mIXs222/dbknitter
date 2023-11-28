from pymongo import MongoClient
from datetime import datetime
import csv

# Constants
MONGO_HOSTNAME = 'mongodb'
MONGO_PORT = 27017
MONGO_DB = 'tpch'
DISCOUNT_RANGE = (0.05, 0.07)
QUANTITY_THRESHOLD = 24
DATE_RANGE = (datetime(1994, 1, 1), datetime(1994, 12, 31))
OUTPUT_FILE = 'query_output.csv'

# Establish a connection to MongoDB
client = MongoClient(host=MONGO_HOSTNAME, port=MONGO_PORT)
db = client[MONGO_DB]

# Construct the query
query = {
    'L_SHIPDATE': {'$gte': DATE_RANGE[0], '$lte': DATE_RANGE[1]},
    'L_DISCOUNT': {'$gte': DISCOUNT_RANGE[0], '$lte': DISCOUNT_RANGE[1]},
    'L_QUANTITY': {'$lt': QUANTITY_THRESHOLD},
}

# Project the required fields
projection = {
    'L_EXTENDEDPRICE': 1,
    'L_DISCOUNT': 1,
}

# Perform the query on collection 'lineitem'
lineitems = db.lineitem.find(query, projection)

# Compute the total revenue
total_revenue = 0
for item in lineitems:
    price = item['L_EXTENDEDPRICE']
    discount = item['L_DISCOUNT']
    total_revenue += price * (1 - discount)

# Write the result to a file
with open(OUTPUT_FILE, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['total_revenue'])
    writer.writerow([total_revenue])
