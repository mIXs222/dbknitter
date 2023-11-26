import pymongo
from bson.son import SON
import direct_redis
import pandas as pd
from datetime import datetime

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb://mongodb:27017/')
mongo_db = mongo_client['tpch']

# Find all parts with P_NAME starting with 'forest'
part_docs = mongo_db['part'].find({'P_NAME': {'$regex': '^forest'}}, {'_id': 0, 'P_PARTKEY': 1})
part_keys = [doc['P_PARTKEY'] for doc in part_docs]

# Find all partsupp entries for these parts and their supply data
partsupp_docs = mongo_db['partsupp'].find(
    {'PS_PARTKEY': {'$in': part_keys}},
    {'_id': 0, 'PS_PARTKEY': 1, 'PS_SUPPKEY': 1, 'PS_AVAILQTY': 1}
)
partsupp_df = pd.DataFrame(list(partsupp_docs))

# Redis connection
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get supplier and lineitem data from Redis
supplier_df = pd.read_json(redis_conn.get('supplier'))
lineitem_df = pd.read_json(redis_conn.get('lineitem'))

# Filter lineitem data for the date range and join with partsupp
lineitem_filtered = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= datetime(1994, 1, 1)) &
    (lineitem_df['L_SHIPDATE'] < datetime(1995, 1, 1))
]
lineitem_grouped = lineitem_filtered.groupby(['L_PARTKEY', 'L_SUPPKEY'])['L_QUANTITY'].sum().reset_index()
lineitem_grouped['half_sum_qty'] = 0.5 * lineitem_grouped['L_QUANTITY']

# Merge partsupp DataFrame with the grouped lineitem data
partsupp_lineitem_df = pd.merge(partsupp_df, lineitem_grouped, how='left', left_on=['PS_PARTKEY', 'PS_SUPPKEY'], right_on=['L_PARTKEY', 'L_SUPPKEY'])
partsupp_lineitem_df = partsupp_lineitem_df[partsupp_lineitem_df['PS_AVAILQTY'] > partsupp_lineitem_df['half_sum_qty']]

# Now that we have the valid PS_SUPPKEYs, get the corresponding suppliers from Redis
valid_suppkeys = partsupp_lineitem_df['PS_SUPPKEY'].unique()
supplier_filtered = supplier_df[supplier_df['S_SUPPKEY'].isin(valid_suppkeys)]

# Get nation data from mongo
nation_df = pd.DataFrame(list(mongo_db['nation'].find({'N_NAME': 'CANADA'}, {'_id': 0, 'N_NATIONKEY': 1})))
supplier_nation_filtered = pd.merge(supplier_filtered, nation_df, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Order the final DataFrame and select columns
final_df = supplier_nation_filtered.sort_values(by='S_NAME')[['S_NAME', 'S_ADDRESS']]

# Write the data to query_output.csv
final_df.to_csv('query_output.csv', index=False)
