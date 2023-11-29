import pymongo
import pandas as pd
from bson.json_util import dumps
from direct_redis import DirectRedis

# Connect to MongoDB
client = pymongo.MongoClient('mongodb', 27017)
mongodb = client.tpch
partsupp_collection = mongodb.partsupp

# Connect to Redis
redis = DirectRedis(host='redis', port=6379, db=0)

# Get partsupp data from MongoDB
partsupp_docs = partsupp_collection.find({})
partsupp_df = pd.DataFrame(list(partsupp_docs))

# Get part and supplier data from Redis
part_redis_data = redis.get('part')
supplier_redis_data = redis.get('supplier')

# Loading Redis data into DataFrames
part_df = pd.read_json(part_redis_data)
supplier_df = pd.read_json(supplier_redis_data)

# Filtering data based on given attributes
filtered_parts = part_df[
    (part_df['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9])) &
    (part_df['P_TYPE'].str.upper() != 'MEDIUM POLISHED') &
    (part_df['P_BRAND'] != 'Brand#45')
]

filtered_suppliers = supplier_df[
    ~(supplier_df['S_COMMENT'].str.contains('Customer.*Complaints', regex=True, case=False))
]

# Merging the data to get suppliers who can supply the required parts
result_df = pd.merge(
    left=filtered_parts,
    right=partsupp_df,
    how='inner',
    left_on='P_PARTKEY',
    right_on='PS_PARTKEY'
)

result_df = pd.merge(
    left=result_df,
    right=filtered_suppliers,
    how='inner',
    left_on='PS_SUPPKEY',
    right_on='S_SUPPKEY'
)

# Aggregating data to get counts
final_result = result_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])['S_SUPPKEY'].nunique().reset_index()
final_result.columns = ['P_BRAND', 'P_TYPE', 'P_SIZE', 'SUPPLIER_COUNT']
final_result.sort_values(by=['SUPPLIER_COUNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True], inplace=True)

# Saving the result to CSV
final_result.to_csv('query_output.csv', index=False)
