# forecasting_revenue_change.py

from pymongo import MongoClient
import csv
from datetime import datetime

# Establish a connection to the MongoDB database
client = MongoClient('mongodb', 27017)
db = client['tpch']
lineitem_collection = db['lineitem']

# Define the range of discounts and quantity
discount_lower_bound = 0.06 - 0.01
discount_upper_bound = 0.06 + 0.01
quantity_upper_bound = 24

# The date range for shipped lineitems
date_lower_bound = datetime(1994, 1, 1)
date_upper_bound = datetime(1995, 1, 1)

# Create the query to find relevant lineitems
query = {
    'L_SHIPDATE': {'$gte': date_lower_bound, '$lt': date_upper_bound},
    'L_DISCOUNT': {'$gte': discount_lower_bound, '$lte': discount_upper_bound},
    'L_QUANTITY': {'$lt': quantity_upper_bound}
}

# Calculate the potential revenue increase
potential_revenue_increase = 0
for lineitem in lineitem_collection.find(query):
    potential_revenue_increase += lineitem['L_EXTENDEDPRICE'] * lineitem['L_DISCOUNT']

# Write the output to a CSV file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['POTENTIAL_REVENUE_INCREASE'])
    writer.writerow([potential_revenue_increase])

# Close the connection to the database
client.close()
