# query.py
from pymongo import MongoClient
import pandas as pd
import csv

# Initialize MongoDB connection
client = MongoClient('mongodb://mongodb:27017/')
db = client['tpch']

# Fetch the data from MongoDB
parts = pd.DataFrame(list(db.part.find(
    {
        'P_BRAND': {'$ne': 'Brand#45'},
        'P_TYPE': {'$not': {'$regex': 'MEDIUM POLISHED.*'}},
        'P_SIZE': {'$in': [49, 14, 23, 45, 19, 3, 36, 9]}
    },
    {'_id': 0, 'P_PARTKEY': 1, 'P_BRAND': 1, 'P_TYPE': 1, 'P_SIZE': 1}
)))

partsupps = pd.DataFrame(list(db.partsupp.find({}, {'_id': 0, 'PS_PARTKEY': 1, 'PS_SUPPKEY': 1})))

suppliers = pd.DataFrame(list(db.supplier.find(
    {'S_COMMENT': {'$not': {'$regex': '.*Customer.*Complaints.*'}}},
    {'_id': 0, 'S_SUPPKEY': 1}
)))
suppliers = set(suppliers['S_SUPPKEY'].tolist())

# Merge part and partsupp collections on P_PARTKEY and PS_PARTKEY
merged_data = pd.merge(parts, partsupps, how='inner', left_on='P_PARTKEY', right_on='PS_PARTKEY')

# Exclude suppliers with any comment about customer complaints
merged_data = merged_data[merged_data['PS_SUPPKEY'].isin(suppliers)]

# Perform the group by and count operation
result = merged_data.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']).agg(SUPPLIER_CNT=('PS_SUPPKEY', 'nunique')).reset_index()

# Sort the result
result = result.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Write the result to a CSV file
result.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
