from pymongo import MongoClient
import csv
from datetime import datetime

# pymongo connection
client = MongoClient('mongodb', 27017)
db = client['tpch']
lineitem_collection = db['lineitem']

# Filter line items with a shipping date on or before September 2, 1998
date_limit = datetime(1998, 9, 2)
query = {"L_SHIPDATE": {"$lte": date_limit}}

# Projection
projection = {
    'L_RETURNFLAG': 1,
    'L_LINESTATUS': 1,
    'L_QUANTITY': 1,
    'L_EXTENDEDPRICE': 1,
    'L_DISCOUNT': 1,
    'L_TAX': 1,
}

# Execute query
lineitem_cursor = lineitem_collection.find(query, projection)

# Aggregation
results = {}
for doc in lineitem_cursor:
    group_key = (doc['L_RETURNFLAG'], doc['L_LINESTATUS'])
    discount_price = doc['L_EXTENDEDPRICE'] * (1 - doc['L_DISCOUNT'])
    charge = discount_price * (1 + doc['L_TAX'])
    
    if group_key not in results:
        results[group_key] = {
            'SUM_QTY': 0,
            'SUM_BASE_PRICE': 0,
            'SUM_DISC_PRICE': 0,
            'SUM_CHARGE': 0,
            'AVG_QTY': [],
            'AVG_PRICE': [],
            'AVG_DISC': [],
            'COUNT_ORDER': 0,
        }
    
    results[group_key]['SUM_QTY'] += doc['L_QUANTITY']
    results[group_key]['SUM_BASE_PRICE'] += doc['L_EXTENDEDPRICE']
    results[group_key]['SUM_DISC_PRICE'] += discount_price
    results[group_key]['SUM_CHARGE'] += charge
    results[group_key]['AVG_QTY'].append(doc['L_QUANTITY'])
    results[group_key]['AVG_PRICE'].append(doc['L_EXTENDEDPRICE'])
    results[group_key]['AVG_DISC'].append(doc['L_DISCOUNT'])
    results[group_key]['COUNT_ORDER'] += 1

# Calculate averages
for group_key in results:
    result = results[group_key]
    result['AVG_QTY'] = sum(result['AVG_QTY']) / result['COUNT_ORDER']
    result['AVG_PRICE'] = sum(result['AVG_PRICE']) / result['COUNT_ORDER']
    result['AVG_DISC'] = sum(result['AVG_DISC']) / result['COUNT_ORDER']

# Write results to csv
output_fields = [
    'L_RETURNFLAG', 'L_LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE',
    'SUM_DISC_PRICE', 'SUM_CHARGE', 'AVG_QTY', 'AVG_PRICE',
    'AVG_DISC', 'COUNT_ORDER'
]
sorted_results = sorted(results.items())

with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=output_fields)
    writer.writeheader()
    for (return_flag, line_status), result in sorted_results:
        row = {
            'L_RETURNFLAG': return_flag,
            'L_LINESTATUS': line_status,
            'SUM_QTY': result['SUM_QTY'],
            'SUM_BASE_PRICE': result['SUM_BASE_PRICE'],
            'SUM_DISC_PRICE': result['SUM_DISC_PRICE'],
            'SUM_CHARGE': result['SUM_CHARGE'],
            'AVG_QTY': result['AVG_QTY'],
            'AVG_PRICE': result['AVG_PRICE'],
            'AVG_DISC': result['AVG_DISC'],
            'COUNT_ORDER': result['COUNT_ORDER'],
        }
        writer.writerow(row)

client.close()
