import pandas as pd
import pymongo
import direct_redis

# Connection details
MONGO_DETAILS = {
    'database': 'tpch',
    'port': 27017,
    'hostname': 'mongodb'
}

REDIS_DETAILS = {
    'database': 0,
    'port': 6379,
    'hostname': 'redis'
}

# Connect to MongoDB
mongo_client = pymongo.MongoClient(f"mongodb://{MONGO_DETAILS['hostname']}:{MONGO_DETAILS['port']}")
mongo_db = mongo_client[MONGO_DETAILS['database']]
supplier_collection = mongo_db['supplier']
lineitem_collection = mongo_db['lineitem']

# Connect to Redis
redis_client = direct_redis.DirectRedis(host=REDIS_DETAILS['hostname'], port=REDIS_DETAILS['port'], db=REDIS_DETAILS['database'])
part_df = pd.read_json(redis_client.get('part'))
partsupp_df = pd.read_json(redis_client.get('partsupp'))
orders_df = pd.read_json(redis_client.get('orders'))
nation_df = pd.read_json(redis_client.get('nation'))

# Query the MongoDB collections
supplier_df = pd.DataFrame(list(supplier_collection.find()))
lineitem_df = pd.DataFrame(list(lineitem_collection.find()))

# Filtering part names containing 'dim'
parts_with_dim = part_df[part_df['P_NAME'].str.contains('dim')]

# Merging parts and partsupp to get supply cost
part_partsupp_merged = parts_with_dim.merge(partsupp_df, left_on='P_PARTKEY', right_on='PS_PARTKEY')

# Merging supplier with the above to get nationkey
lineitem_supplier_merged = lineitem_df.merge(
    supplier_df,
    left_on='L_SUPPKEY',
    right_on='S_SUPPKEY'
)

# Merging with part_partsupp_merged to calculate profit
profit_calculation_df = lineitem_supplier_merged.merge(
    part_partsupp_merged,
    left_on=['L_PARTKEY', 'L_SUPPKEY'],
    right_on=['P_PARTKEY', 'PS_SUPPKEY']
)

# Calculate profit for each lineitem
profit_calculation_df['PROFIT'] = (profit_calculation_df['L_EXTENDEDPRICE'] * (1 - profit_calculation_df['L_DISCOUNT'])) - (profit_calculation_df['PS_SUPPLYCOST'] * profit_calculation_df['L_QUANTITY'])

# Merging with orders to get date and nation
final_df = profit_calculation_df.merge(
    orders_df,
    left_on='L_ORDERKEY',
    right_on='O_ORDERKEY'
)

# Extracting year from O_ORDERDATE
final_df['YEAR'] = pd.to_datetime(final_df['O_ORDERDATE']).dt.year

# Merging with nations to get nation names
result_df = final_df.merge(
    nation_df,
    left_on='S_NATIONKEY',
    right_on='N_NATIONKEY'
)

# Grouping by Nation and Year
grouped_result = result_df.groupby(['N_NAME', 'YEAR'])['PROFIT'].sum().reset_index()

# Sorting the results
sorted_result = grouped_result.sort_values(by=['N_NAME', 'YEAR'], ascending=[True, False])

# Writing to CSV
sorted_result.to_csv('query_output.csv', index=False)
