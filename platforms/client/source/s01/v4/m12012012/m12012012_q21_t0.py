import pymongo
import pandas as pd
from direct_redis import DirectRedis
import csv

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]

# Query for nation table
nations = pd.DataFrame(list(mongo_db["nation"].find({"N_NAME": "SAUDI ARABIA"})))

# Query for supplier table
suppliers = pd.DataFrame(list(mongo_db["supplier"].find()))

# Query for orders tables
orders = pd.DataFrame(list(mongo_db["orders"].find({"O_ORDERSTATUS": "F"})))

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Read lineitem table from Redis
lineitem_df = redis_client.get('lineitem')

# Convert lineitem to DataFrame
lineitems = pd.read_json(lineitem_df)

# Perform join operations
merged_df = suppliers.merge(lineitems, how='inner', left_on='S_SUPPKEY', right_on='L_SUPPKEY')
merged_df = merged_df.merge(orders, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')
merged_df = merged_df.merge(nations, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Filter the data according to the WHERE conditions
filtered_df = merged_df[(merged_df['L_RECEIPTDATE'] > merged_df['L_COMMITDATE'])]
filtered_df = filtered_df.drop_duplicates(subset=['L_ORDERKEY'])

# Applying the EXISTS and NOT EXISTS conditions
l1_df = filtered_df[['L_ORDERKEY', 'L_SUPPKEY']].rename(columns={'L_SUPPKEY': 'L1_SUPPKEY'})
l2_df = lineitems[['L_ORDERKEY', 'L_SUPPKEY']].rename(columns={'L_SUPPKEY': 'L2_SUPPKEY'})

# Performing EXISTS sub-query equivalent
exists_df = l1_df.merge(l2_df, left_on='L_ORDERKEY', right_on='L_ORDERKEY')
exists_df = exists_df[exists_df['L1_SUPPKEY'] != exists_df['L2_SUPPKEY']]

# Performing NOT EXISTS sub-query equivalent
not_exists_df = exists_df.merge(filtered_df[['L_ORDERKEY', 'L_RECEIPTDATE', 'L_COMMITDATE']], left_on='L_ORDERKEY', right_on='L_ORDERKEY', how='left')
not_exists_df = not_exists_df[~(not_exists_df['L_RECEIPTDATE'] > not_exists_df['L_COMMITDATE'])]

# Do final group by and count
final_df = not_exists_df.groupby('S_NAME').size().reset_index(name='NUMWAIT')

# Sort the results
final_df.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True], inplace=True)

# Write the output to a CSV file
final_df.to_csv('query_output.csv', index=False)
