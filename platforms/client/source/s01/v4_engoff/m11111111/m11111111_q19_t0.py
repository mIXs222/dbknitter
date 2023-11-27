import pymongo
import pandas as pd
import csv

# Establish connection to MongoDB
client = pymongo.MongoClient('mongodb', 27017)
db = client['tpch']

# Define the conditions for the different part types
conditions = [
    {
        'P_BRAND': 'Brand#12',
        'P_CONTAINER': {'$in': ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']},
        'P_SIZE': {'$gte': 1, '$lte': 5},
        'L_QUANTITY': {'$gte': 1, '$lte': 11},
    },
    {
        'P_BRAND': 'Brand#23',
        'P_CONTAINER': {'$in': ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK']},
        'P_SIZE': {'$gte': 1, '$lte': 10},
        'L_QUANTITY': {'$gte': 10, '$lte': 20},
    },
    {
        'P_BRAND': 'Brand#34',
        'P_CONTAINER': {'$in': ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']},
        'P_SIZE': {'$gte': 1, '$lte': 15},
        'L_QUANTITY': {'$gte': 20, '$lte': 30},
    }
]

shipmode_condition = {'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']}}

# Find parts that satisfy the given conditions and join with lineitems
all_results = []
for condition in conditions:
    parts_cursor = db.part.find(condition, {'P_PARTKEY': 1})
    part_keys = [part['P_PARTKEY'] for part in parts_cursor]

    lineitem_cursor = db.lineitem.find({
        'L_PARTKEY': {'$in': part_keys},
        **shipmode_condition
    })

    for lineitem in lineitem_cursor:
        discounted_revenue = lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])
        result = {
            'L_ORDERKEY': lineitem['L_ORDERKEY'],
            'L_PARTKEY': lineitem['L_PARTKEY'],
            'L_SUPPKEY': lineitem['L_SUPPKEY'],
            'L_LINENUMBER': lineitem['L_LINENUMBER'],
            'L_QUANTITY': lineitem['L_QUANTITY'],
            'L_EXTENDEDPRICE': lineitem['L_EXTENDEDPRICE'],
            'L_DISCOUNT': lineitem['L_DISCOUNT'],
            'L_TAX': lineitem['L_TAX'],
            'DISCOUNTED_REVENUE': discounted_revenue,
        }
        all_results.append(result)

# Convert results to a pandas DataFrame
df_results = pd.DataFrame(all_results)

# Save results to CSV file
df_results.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
