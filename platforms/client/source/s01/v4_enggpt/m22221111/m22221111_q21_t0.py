# analysis.py
from pymongo import MongoClient
from direct_redis import DirectRedis
import pandas as pd

# MongoDB connection and data retrieval
mongo_client = MongoClient(host='mongodb', port=27017)
mongodb = mongo_client['tpch']
orders_df = pd.DataFrame(list(mongodb.orders.find({'O_ORDERSTATUS': 'F'})))
lineitem_df = pd.DataFrame(list(mongodb.lineitem.find({'$expr': {'$gt': ['$L_RECEIPTDATE', '$L_COMMITDATE']}})))

# Redis connection and data retrieval
redis_client = DirectRedis(host='redis', port=6379, db=0)
nation_df = pd.read_msgpack(redis_client.get('nation'))
supplier_df = pd.read_msgpack(redis_client.get('supplier'))

# Filter suppliers from Saudi Arabia
nation_sa = nation_df[nation_df['N_NAME'] == 'SAUDI ARABIA']
sa_suppliers = supplier_df[supplier_df['S_NATIONKEY'].isin(nation_sa['N_NATIONKEY'])]

# Merge dataframes
merged_df = sa_suppliers.merge(lineitem_df, left_on='S_SUPPKEY', right_on='L_SUPPKEY').merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Perform analysis
results = merged_df.groupby('S_NAME').agg(NUMWAIT=('L_ORDERKEY', 'count')).reset_index()

# Filter based on EXISTS subqueries conditions
def exists_subquery(group):
    keys = group[['L_ORDERKEY', 'S_SUPPKEY']].drop_duplicates()
    exists1 = keys.apply(lambda x: (group['L_ORDERKEY'] == x['L_ORDERKEY']) & (group['S_SUPPKEY'] != x['S_SUPPKEY']), axis=1).any()
    exists2 = keys.apply(lambda x: not (group[(group['L_ORDERKEY'] == x['L_ORDERKEY']) & (group['S_SUPPKEY'] != x['S_SUPPKEY']) & (group['L_RECEIPTDATE'] > group['L_COMMITDATE'])].empty), axis=1).all()
    return exists1 and not exists2

filtered_results = results[results.groupby('S_NAME').apply(exists_subquery)]

# Sort the results
sorted_results = filtered_results.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Write to CSV
sorted_results.to_csv('query_output.csv', index=False)

# Close database connections
mongo_client.close()
redis_client.close()
