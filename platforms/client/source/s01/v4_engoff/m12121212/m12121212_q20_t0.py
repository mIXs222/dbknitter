import pymongo
from pymongo import MongoClient
import direct_redis
import pandas as pd
from datetime import datetime

# Connection to MongoDB
client = MongoClient('mongodb', 27017)
mdb = client['tpch']
nation = pd.DataFrame(list(mdb.nation.find({}, {'_id': 0})))
part = pd.DataFrame(list(mdb.part.find({}, {'_id': 0})))
partsupp = pd.DataFrame(list(mdb.partsupp.find({}, {'_id': 0})))

# Filter parts with "forest" in their names
forest_parts = part[part['P_NAME'].str.contains('forest', case=False)]

# Connection to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
supplier = pd.read_json(r.get('supplier'))
lineitem = pd.read_json(r.get('lineitem'))

# Convert string dates to datetime
lineitem['L_SHIPDATE'] = pd.to_datetime(lineitem['L_SHIPDATE'])

# Filter line items shipped between 1994 and 1995 for Canada
canada_nations = nation[nation['N_NAME'].str.lower() == 'canada']['N_NATIONKEY']
supplier_canada = supplier[supplier['S_NATIONKEY'].isin(canada_nations)]
lineitem_filtered = lineitem[
    (lineitem['L_SHIPDATE'] >= datetime(1994, 1, 1)) &
    (lineitem['L_SHIPDATE'] < datetime(1995, 1, 1)) &
    (lineitem['L_SUPPKEY'].isin(supplier_canada['S_SUPPKEY']))
]

# Join on partsupp to get part keys
lineitem_parts = pd.merge(
    lineitem_filtered,
    partsupp[['PS_PARTKEY', 'PS_SUPPKEY']],
    left_on=['L_PARTKEY', 'L_SUPPKEY'],
    right_on=['PS_PARTKEY', 'PS_SUPPKEY']
)

# Join with forest parts
potential_promo_parts = pd.merge(
    lineitem_parts,
    forest_parts[['P_PARTKEY']],
    left_on='PS_PARTKEY',
    right_on='P_PARTKEY'
)

# Group by supplier and calculate excess
excess_suppliers = potential_promo_parts.groupby('L_SUPPKEY').agg({
    'L_QUANTITY': 'sum'
}).reset_index()

excess_suppliers = excess_suppliers[excess_suppliers['L_QUANTITY'] > forest_parts['P_SIZE'].sum() * 0.5]

# Merge to get supplier details
excess_suppliers_details = pd.merge(
    excess_suppliers,
    supplier_canada,
    left_on='L_SUPPKEY',
    right_on='S_SUPPKEY'
)

# Write to CSV
excess_suppliers_details.to_csv('query_output.csv', index=False)
