# python code to execute the query
import pymongo
import redis
import pandas as pd

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
mongo_supplier = mongo_db["supplier"].find()
mongo_nation = mongo_db["nation"].find({'N_NAME': 'SAUDI ARABIA'})

# Filter suppliers by nation and transform to DataFrame
supplier_df = pd.DataFrame(list(mongo_supplier))
nation_df = pd.DataFrame(list(mongo_nation))
nation_supplier_df = supplier_df[supplier_df['S_NATIONKEY'].isin(nation_df['N_NATIONKEY'])]

# Connect to Redis
redis_client = redis.StrictRedis(host='redis', port=6379, db=0)

# Load data from Redis into DataFrames
orders_df = pd.DataFrame(redis_client.get('orders'))
lineitem_df = pd.DataFrame(redis_client.get('lineitem'))

# Create L1 alias for lineitem DataFrame
L1 = lineitem_df.rename(columns=lambda x: 'L1_' + x)

# Process the query
merged_df = nation_supplier_df.merge(
    L1[L1['L1_RECEIPTDATE'] > L1['L1_COMMITDATE']],
    left_on='S_SUPPKEY',
    right_on='L1_SUPPKEY'
)

# Process EXISTS subquery
exists_df = lineitem_df.rename(columns=lambda x: 'L2_' + x)
merged_df = merged_df.merge(
    exists_df,
    left_on='L1_ORDERKEY',
    right_on='L2_ORDERKEY',
    how='inner'
)
merged_df = merged_df[merged_df['L1_SUPPKEY'] != merged_df['L2_SUPPKEY']]

# Process NOT EXISTS subquery
not_exists_df = lineitem_df.rename(columns=lambda x: 'L3_' + x)
not_exists_df = not_exists_df[not_exists_df['L3_RECEIPTDATE'] > not_exists_df['L3_COMMITDATE']]
merged_df = merged_df.merge(
    not_exists_df,
    left_on='L1_ORDERKEY',
    right_on='L3_ORDERKEY',
    how='left',
    indicator=True
)
merged_df = merged_df[merged_df['_merge'] == 'left_only']

# Merge with orders DataFrame
final_df = merged_df.merge(
    orders_df[orders_df['O_ORDERSTATUS'] == 'F'],
    left_on='L1_ORDERKEY',
    right_on='O_ORDERKEY'
)

# Group by S_NAME and count
result_df = final_df.groupby('S_NAME').size().reset_index(name='NUMWAIT')
result_df = result_df.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Write the result to a CSV file
result_df.to_csv('query_output.csv', index=False)

# Clean up, close connections
mongo_client.close()
