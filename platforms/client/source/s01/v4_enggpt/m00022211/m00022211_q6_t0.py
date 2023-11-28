from pymongo import MongoClient
import csv

# MongoDB connection parameters
MONGODB_HOST = 'mongodb'
MONGODB_PORT = 27017
MONGODB_DB_NAME = 'tpch'

# Connect to MongoDB
client = MongoClient(host=MONGODB_HOST, port=MONGODB_PORT)
db = client[MONGODB_DB_NAME]

# Define the query
query = {
    'L_SHIPDATE': {'$gte': '1994-01-01', '$lte': '1994-12-31'},
    'L_DISCOUNT': {'$gte': 0.05, '$lte': 0.07},
    'L_QUANTITY': {'$lt': 24}
}

# Project the necessary fields
projection = {
    'L_EXTENDEDPRICE': 1,
    'L_DISCOUNT': 1,
    '_id': 0
}

# Execute the query and calculate the total revenue
total_revenue = 0
for document in db.lineitem.find(query, projection):
    total_revenue += document['L_EXTENDEDPRICE'] * (1 - document['L_DISCOUNT'])

# Write the result to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Total Revenue'])
    writer.writerow([total_revenue])

print("The total revenue has been written to query_output.csv")
