import pandas as pd
import pymongo
from direct_redis import DirectRedis

# MongoDB Connection
mongodb_client = pymongo.MongoClient('mongodb', port=27017)
mongodb_db = mongodb_client['tpch']

# Load MongoDB tables
nation = pd.DataFrame(list(mongodb_db.nation.find({}, {'_id': 0})))
part = pd.DataFrame(list(mongodb_db.part.find({}, {'_id': 0})))
partsupp = pd.DataFrame(list(mongodb_db.partsupp.find({}, {'_id': 0})))
orders = pd.DataFrame(list(mongodb_db.orders.find({}, {'_id': 0})))

# Redis Connection
redis = DirectRedis(host='redis', port=6379, db=0)

# Load Redis tables
supplier = pd.read_json(redis.get('supplier'))
lineitem = pd.read_json(redis.get('lineitem'))

# Filtering parts
specified_dim = 'SPECIFIED_DIM'
filtered_parts = part[part['P_NAME'].str.contains(specified_dim)]
lineitem_filtered = lineitem[lineitem['L_PARTKEY'].isin(filtered_parts['P_PARTKEY'])]

# Join with supplier to filter by nation
lineitem_supp = pd.merge(lineitem_filtered, supplier, how='left', left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Join with nation
lineitem_supp_nation = pd.merge(lineitem_supp, nation, how='left', left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Join with partsupp to get supply cost
lineitem_full = pd.merge(lineitem_supp_nation, partsupp, how='left', left_on=['L_PARTKEY', 'L_SUPPKEY'],
                         right_on=['PS_PARTKEY', 'PS_SUPPKEY'])

# Calculate profit
lineitem_full['profit'] = (lineitem_full['L_EXTENDEDPRICE'] * (1 - lineitem_full['L_DISCOUNT'])) - \
                          (lineitem_full['PS_SUPPLYCOST'] * lineitem_full['L_QUANTITY'])

# Extract year from order date
lineitem_full['year'] = pd.to_datetime(lineitem_full['L_SHIPDATE']).dt.year

# Group by nation and year
grouped = lineitem_full.groupby(['N_NAME', 'year'])['profit'].sum().reset_index()

# Sort the result
result = grouped.sort_values(by=['N_NAME', 'year'], ascending=[True, False])

# Write to CSV
result.to_csv('query_output.csv', index=False)
