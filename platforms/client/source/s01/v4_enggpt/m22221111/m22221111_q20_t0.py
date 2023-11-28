import pymongo
import direct_redis
import pandas as pd
from datetime import datetime

# Set up a connection to the MongoDB database
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Set up a connection to the Redis database
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Load data from MongoDB
partsupp_df = pd.DataFrame(list(mongodb['partsupp'].find()))
lineitem_df = pd.DataFrame(list(mongodb['lineitem'].find({}, {'L_PARTKEY': 1, 'L_SUPPKEY': 1, 'L_QUANTITY': 1, 'L_SHIPDATE': 1})))

# Load data from Redis
nation_df = pd.read_json(r.get('nation'))
supplier_df = pd.read_json(r.get('supplier'))
part_df = pd.read_json(r.get('part'))

# Process Redis data
# 1. Filter for suppliers in Canada
canada_nationkey = nation_df[nation_df['N_NAME'] == 'CANADA']['N_NATIONKEY'].iloc[0]
suppliers_in_canada_df = supplier_df[supplier_df['S_NATIONKEY'] == canada_nationkey]

# Process MongoDB data
# 2. Filter parts that start with 'forest'
part_keys = part_df[part_df['P_NAME'].str.startswith('forest')]['P_PARTKEY']

# Join partsupp with part keys on PS_PARTKEY
partsupp_with_forest_parts_df = partsupp_df[partsupp_df['PS_PARTKEY'].isin(part_keys)]

# 3. Sum of line item quantities with the condition and calculate threshold
filtered_lineitems_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= datetime.strptime('1994-01-01', '%Y-%m-%d')) &
    (lineitem_df['L_SHIPDATE'] <= datetime.strptime('1995-01-01', '%Y-%m-%d'))
]
grouped_lineitems = filtered_lineitems_df.groupby(['L_PARTKEY', 'L_SUPPKEY'])['L_QUANTITY'].sum().reset_index()
grouped_lineitems['threshold'] = grouped_lineitems['L_QUANTITY'] / 2

# Select partsupplies that match the threshold and exist in grouped_lineitems
parts_in_threshold_df = partsupp_with_forest_parts_df.merge(
    grouped_lineitems,
    how='inner',
    left_on=['PS_PARTKEY', 'PS_SUPPKEY'],
    right_on=['L_PARTKEY', 'L_SUPPKEY']
)
parts_in_threshold_df = parts_in_threshold_df[
    parts_in_threshold_df['PS_AVAILQTY'] >= parts_in_threshold_df['threshold']
]

# Extract the S_SUPPKEY that match the conditions
supplier_keys = parts_in_threshold_df['PS_SUPPKEY'].unique()

# Final result
final_suppliers_df = suppliers_in_canada_df[suppliers_in_canada_df['S_SUPPKEY'].isin(supplier_keys)]
final_suppliers_df = final_suppliers_df[['S_NAME', 'S_ADDRESS']].sort_values(by='S_NAME').reset_index(drop=True)

# Output the result to a CSV file
final_suppliers_df.to_csv('query_output.csv', index=False)
