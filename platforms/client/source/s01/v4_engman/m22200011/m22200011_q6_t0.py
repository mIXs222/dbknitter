# query.py
from pymongo import MongoClient
import csv

# Connection to MongoDB
client = MongoClient('mongodb', 27017)
db = client.tpch
lineitem_collection = db.lineitem

# MongoDB query
query = {
    'L_SHIPDATE': {'$gt': '1994-01-01', '$lt': '1995-01-01'},
    'L_DISCOUNT': {'$gt': 0.05, '$lt': 0.07},
    'L_QUANTITY': {'$lt': 24}
}

# Project only the relevant fields for calculation
projection = {
    'L_EXTENDEDPRICE': 1,
    'L_DISCOUNT': 1,
    '_id': 0
}

# Performing the query and calculating the revenue on the fly
revenue = 0
for lineitem in lineitem_collection.find(query, projection):
    revenue += lineitem['L_EXTENDEDPRICE'] * lineitem['L_DISCOUNT']

# Writing the output to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['REVENUE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerow({'REVENUE': revenue})
