import pymongo
import pandas as pd
import redis
from direct_redis import DirectRedis

# Connect to MongoDB
client = pymongo.MongoClient('mongodb://mongodb:27017/')
mongo_db = client['tpch']

# Fetch data from MongoDB
supplier_df = pd.DataFrame(list(mongo_db.supplier.find(
    {'$and': [
        {'S_COMMENT': {'$not': {'$regex': '.*complaints.*', '$options': 'i'}}}
    ]}
)))
partsupp_df = pd.DataFrame(list(mongo_db.partsupp.find()))

# Connect to Redis
r = DirectRedis(host='redis', port=6379, db=0)

# Fetch data from Redis and convert bytes to string
part_raw = r.get('part')
part_str = part_raw.decode('utf-8')

# Read part data from string to pandas DataFrame
from io import StringIO
part_df = pd.read_csv(StringIO(part_str))

# Apply the necessary filters on part_df
filtered_parts = part_df[
    ~part_df.P_BRAND.eq('Brand#45') &
    ~part_df.P_TYPE.str.contains('MEDIUM POLISHED') &
    part_df.P_SIZE.isin([49, 14, 23, 45, 19, 3, 36, 9])
]

# Merge the supplier and partsupp dataframes
merged_df = pd.merge(
    partsupp_df,
    supplier_df,
    how='inner',
    left_on='PS_SUPPKEY',
    right_on='S_SUPPKEY'
)

# Merge the merged_df with filtered_parts
final_df = pd.merge(
    merged_df,
    filtered_parts,
    how='inner',
    left_on='PS_PARTKEY',
    right_on='P_PARTKEY'
)

# Aggregate the final result
final_result = (
    final_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])
    .agg({'S_SUPPKEY': 'nunique'})
    .rename(columns={'S_SUPPKEY': 'SUPPLIER_COUNT'})
    .sort_values(['SUPPLIER_COUNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])
    .reset_index()
)

# Output to CSV file
final_result.to_csv('query_output.csv', index=False)
