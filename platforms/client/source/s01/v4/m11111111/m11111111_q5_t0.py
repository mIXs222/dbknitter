# query.py
import pymongo
import csv
from datetime import datetime

# Connect to MongoDB
client = pymongo.MongoClient('mongodb', 27017)
db = client['tpch']

# Find all nations and regions
nations = {doc['N_NATIONKEY']: doc for doc in db['nation'].find()}
regions = {doc['R_REGIONKEY']: doc for doc in db['region'].find()}

# Filter nations by region 'ASIA'
asia_nations = [n for n, r in nations.items() if regions[r['N_REGIONKEY']]['R_NAME'] == 'ASIA']

# Filter suppliers by the nation keys from the asia_nations list
suppliers = {doc['S_SUPPKEY']: doc for doc in db['supplier'].find({'S_NATIONKEY': {'$in': asia_nations}})}

# Define date range
date_start = datetime(1990, 1, 1)
date_end = datetime(1995, 1, 1)

# Find all customers that are connected to the Asian nations
customers = {doc['C_CUSTKEY']: doc 
             for doc in db['customer'].find({'C_NATIONKEY': {'$in': asia_nations}})}

# Find all orders made by those customers in the given date range
orders = {doc['O_ORDERKEY']: doc 
          for doc in db['orders'].find({'O_CUSTKEY': {'$in': list(customers.keys())},
                                        'O_ORDERDATE': {'$gte': date_start, '$lt': date_end}})}

# Initialize the results dictionary
results = {}

# Iterate over lineitems, calculate revenue and group by nation name
for lineitem in db['lineitem'].find({'L_ORDERKEY': {'$in': list(orders.keys())}}):
    order = orders[lineitem['L_ORDERKEY']]
    supplier = suppliers[lineitem['L_SUPPKEY']]
    nation = nations[supplier['S_NATIONKEY']]
    revenue = lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])

    if nation['N_NAME'] not in results:
        results[nation['N_NAME']] = 0
    results[nation['N_NAME']] += revenue

# Sort the results
sorted_results = sorted(results.items(), key=lambda item: item[1], reverse=True)

# Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['N_NAME', 'REVENUE'])  # Header
    for n_name, revenue in sorted_results:
        csvwriter.writerow([n_name, round(revenue, 2)])  # Rounded to 2 decimal places

client.close()
