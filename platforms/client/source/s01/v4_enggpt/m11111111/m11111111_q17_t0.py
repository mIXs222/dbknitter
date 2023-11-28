import csv
import pymongo
from bson.decimal128 import Decimal128
from pymongo import MongoClient
from decimal import Decimal

# Connect to the MongoDB database
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Get the relevant collections
part_collection = db['part']
lineitem_collection = db['lineitem']

# Identify parts with brand 'Brand#23' and container 'MED BAG'
target_parts = part_collection.find(
    {'P_BRAND': 'Brand#23', 'P_CONTAINER': 'MED BAG'},
    {'P_PARTKEY': 1}
)

partkeys_of_interest = [part['P_PARTKEY'] for part in target_parts]

# Calculate the average quantity for the parts of interest
average_quantities = {}
for partkey in partkeys_of_interest:
    avg_quantity = lineitem_collection.aggregate([
        {'$match': {'L_PARTKEY': partkey}},
        {'$group': {'_id': None, 'avgQuantity': {'$avg': '$L_QUANTITY'}}}
    ])
    average_quantities[partkey] = next(avg_quantity)['avgQuantity'] * Decimal('0.2')

# Retrieve line items matching the criteria
lines_of_interest = lineitem_collection.find({
    'L_PARTKEY': {'$in': partkeys_of_interest},
    'L_QUANTITY': {'$lt': Decimal128('0')}  # Placeholder condition to modify later
})

# Process each line item and calculate the yearly average extended price
total_extended_price = Decimal('0')
for line in lines_of_interest:
    if Decimal(str(line['L_QUANTITY'])) < average_quantities[line['L_PARTKEY']]:
        total_extended_price += Decimal(str(line['L_EXTENDEDPRICE']))

# Calculate the average yearly extended price
if total_extended_price != Decimal('0'):
    average_yearly_extended_price = total_extended_price / Decimal('7.0')
else:
    average_yearly_extended_price = Decimal('0')

# Write the result to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['AverageYearlyExtendedPrice'])
    csvwriter.writerow([str(average_yearly_extended_price)])
