# import necessary libraries
import pymongo
from direct_redis import DirectRedis
import pandas as pd

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = client["tpch"]

# Load MongoDB collections
nation = pd.DataFrame(list(mongodb["nation"].find({}, {'_id': False})))
part = pd.DataFrame(list(mongodb["part"].find({'P_NAME': {'$regex': '.*dim.*'}}, {'_id': False})))
partsupp = pd.DataFrame(list(mongodb["partsupp"].find({}, {'_id': False})))
orders = pd.DataFrame(list(mongodb["orders"].find({}, {'_id': False})))

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Load Redis tables
supplier = pd.read_json(redis_conn.get('supplier'))
lineitem = pd.read_json(redis_conn.get('lineitem'))

# Perform the SQL-like joins and operations with pandas
result_df = pd.merge(supplier, lineitem, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
result_df = pd.merge(result_df, partsupp, left_on=['L_SUPPKEY', 'L_PARTKEY'], right_on=['PS_SUPPKEY', 'PS_PARTKEY'])
result_df = pd.merge(result_df, part, left_on='L_PARTKEY', right_on='P_PARTKEY')
result_df = pd.merge(result_df, orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
result_df = pd.merge(result_df, nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Calculate necessary fields and aggregate
result_df['O_YEAR'] = pd.to_datetime(result_df['O_ORDERDATE']).dt.year
result_df['AMOUNT'] = result_df['L_EXTENDEDPRICE'] * (1 - result_df['L_DISCOUNT']) - result_df['PS_SUPPLYCOST'] * result_df['L_QUANTITY']
agg_df = result_df.groupby(['N_NAME', 'O_YEAR']).agg(SUM_PROFIT=('AMOUNT', 'sum')).reset_index()

# Rename columns to match the SQL query's output
agg_df.rename(columns={'N_NAME': 'NATION'}, inplace=True)

# Order the results
agg_df.sort_values(['NATION', 'O_YEAR'], ascending=[True, False], inplace=True)

# Write the output to a CSV file
agg_df.to_csv('query_output.csv', index=False)
