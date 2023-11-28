import pymongo
import pandas as pd
from datetime import datetime
import direct_redis

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
mongo_supplier = pd.DataFrame(list(mongo_db["supplier"].find({}, {'_id': False})))
mongo_lineitem = pd.DataFrame(list(mongo_db["lineitem"].find({}, {'_id': False})))

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
nation_data = redis_client.get('nation')
part_data = redis_client.get('part')
partsupp_data = redis_client.get('partsupp')

# Convert Redis data to Pandas DataFrame
nation_df = pd.read_msgpack(nation_data)
part_df = pd.read_msgpack(part_data)
partsupp_df = pd.read_msgpack(partsupp_data)

# Filter nation to get Canada
nation_canada_df = nation_df[nation_df['N_NAME'] == 'CANADA']

# Filter part names that start with 'forest' and join with partsupp
part_forest_df = part_df[part_df['P_NAME'].str.startswith('forest')]
partsupp_forest_df = partsupp_df[partsupp_df['PS_PARTKEY'].isin(part_forest_df['P_PARTKEY'])]

# Calculate the sum of line item quantities for a specific part-supplier combination
lineitem_filtered = mongo_lineitem[
    (mongo_lineitem['L_SHIPDATE'] >= datetime.strptime('1994-01-01', '%Y-%m-%d')) &
    (mongo_lineitem['L_SHIPDATE'] <= datetime.strptime('1995-01-01', '%Y-%m-%d'))
]
lineitem_grouped = lineitem_filtered.groupby(['L_PARTKEY', 'L_SUPPKEY'])['L_QUANTITY'].sum().reset_index()
lineitem_grouped['THRESHOLD_QUANTITY'] = lineitem_grouped['L_QUANTITY'] / 2

# Join the filtered partsupp with lineitem to identify suppliers meeting the threshold
supplier_keys_meeting_threshold = partsupp_forest_df.merge(
    lineitem_grouped,
    left_on=['PS_PARTKEY', 'PS_SUPPKEY'],
    right_on=['L_PARTKEY', 'L_SUPPKEY']
)
supplier_keys_meeting_threshold = supplier_keys_meeting_threshold[
    supplier_keys_meeting_threshold['PS_AVAILQTY'] > supplier_keys_meeting_threshold['THRESHOLD_QUANTITY']
]

# Merge suppliers with nations and filter by the nation 'CANADA'
supplier_nation_df = mongo_supplier.merge(nation_canada_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
# Filter suppliers by supplier keys that meet threshold and specific nation
filtered_suppliers_df = supplier_nation_df[
    supplier_nation_df['S_SUPPKEY'].isin(supplier_keys_meeting_threshold['PS_SUPPKEY'])
]

# Selecting the required columns and sorting by S_NAME
output_df = filtered_suppliers_df[['S_NAME', 'S_ADDRESS']].sort_values('S_NAME')

# Write to CSV
output_df.to_csv('query_output.csv', index=False)
