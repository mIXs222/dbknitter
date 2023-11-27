# revenue_query.py
from pymongo import MongoClient
import csv

# Connect to the MongoDB server
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Define the query
query = {
    'L_SHIPDATE': {'$gte': '1994-01-01', '$lt': '1995-01-01'},
    'L_DISCOUNT': {'$gte': .05, '$lte': .07},
    'L_QUANTITY': {'$lt': 24}
}

# Project required fields
projection = {
    'L_EXTENDEDPRICE': 1,
    'L_DISCOUNT': 1,
    '_id': 0
}

# Execute the query and calculate revenue
revenue = 0
for document in db.lineitem.find(query, projection):
    revenue += document['L_EXTENDEDPRICE'] * document['L_DISCOUNT']

# Write the result to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['REVENUE'])
    writer.writerow([revenue])

print(f"Revenue calculated and written to query_output.csv: {revenue}")
