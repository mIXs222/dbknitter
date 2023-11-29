# query.py
from pymongo import MongoClient
import csv
from datetime import datetime

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Define the query filter and projection
filter = {"L_SHIPDATE": {"$lt": datetime(1998, 9, 2)}}
projection = {
    "L_QUANTITY": 1, "L_EXTENDEDPRICE": 1, "L_DISCOUNT": 1,
    "L_TAX": 1, "L_RETURNFLAG": 1, "L_LINESTATUS": 1
}

# Execute the query
lineitems = db.lineitem.find(filter, projection)

# Calculate aggregates
aggregates = {}
for item in lineitems:
    flag_status = (item['L_RETURNFLAG'], item['L_LINESTATUS'])
    if flag_status not in aggregates:
        aggregates[flag_status] = {
            'sum_qty': 0,
            'sum_base_price': 0,
            'sum_disc_price': 0,
            'sum_charge': 0,
            'avg_qty': 0,
            'avg_price': 0,
            'avg_disc': 0,
            'count_order': 0
        }
    aggr = aggregates[flag_status]
    disc_price = item['L_EXTENDEDPRICE'] * (1 - item['L_DISCOUNT'])
    charge = disc_price * (1 + item['L_TAX'])

    aggr['sum_qty'] += item['L_QUANTITY']
    aggr['sum_base_price'] += item['L_EXTENDEDPRICE']
    aggr['sum_disc_price'] += disc_price
    aggr['sum_charge'] += charge
    aggr['avg_qty'] += item['L_QUANTITY']
    aggr['avg_price'] += item['L_EXTENDEDPRICE']
    aggr['avg_disc'] += item['L_DISCOUNT']
    aggr['count_order'] += 1

# Finalize averages
for aggr in aggregates.values():
    aggr['avg_qty'] /= aggr['count_order']
    aggr['avg_price'] /= aggr['count_order']
    aggr['avg_disc'] /= aggr['count_order']

# Sort results
sorted_results = sorted(aggregates.items(), key=lambda x: x[0])

# Write to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow([
        'RETURNFLAG', 'LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE', 'SUM_DISC_PRICE',
        'SUM_CHARGE', 'AVG_QTY', 'AVG_PRICE', 'AVG_DISC', 'COUNT_ORDER'
    ])
    for key, aggr in sorted_results:
        csvwriter.writerow([
            key[0], key[1], aggr['sum_qty'], aggr['sum_base_price'], aggr['sum_disc_price'],
            aggr['sum_charge'], aggr['avg_qty'], aggr['avg_price'], aggr['avg_disc'],
            aggr['count_order']
        ])
