from pymongo import MongoClient
import csv
from datetime import datetime

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']
lineitem_collection = db['lineitem']

# Query parameters
start_date = datetime(1994, 1, 1)
end_date = datetime(1995, 1, 1)
min_discount = 0.06 - 0.01
max_discount = 0.06 + 0.01
max_quantity = 24

# MongoDB query
query = {
    'L_SHIPDATE': {'$gte': start_date, '$lt': end_date},
    'L_DISCOUNT': {'$gte': min_discount, '$lte': max_discount},
    'L_QUANTITY': {'$lt': max_quantity}
}

# Execute query and calculate revenue change
revenue_change = 0
for line in lineitem_collection.find(query):
    revenue_change += line['L_EXTENDEDPRICE'] * line['L_DISCOUNT']

# Write the output to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['REVENUE_CHANGE'])
    writer.writerow([revenue_change])
