# Python code to execute the query across different databases and save the result to 'query_output.csv'

import pandas as pd
import pymysql
from pymongo import MongoClient
import redis
import direct_redis
from sqlalchemy import create_engine

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch', charset='utf8mb4')
mysql_engine = create_engine("mysql+pymysql://root:my-secret-pw@mysql/tpch")

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch data from MySQL
lineitem_df = pd.read_sql('SELECT * FROM lineitem', con=mysql_engine)
partsupp_df = pd.read_sql('SELECT * FROM partsupp', con=mysql_engine)

# Fetch data from MongoDB
part_df = pd.DataFrame(list(mongo_db.part.find({"P_NAME": {"$regex": ".*dim.*"}})))

# Fetch data from Redis
nation_df = pd.read_json(redis_conn.get('nation'))
supplier_df = pd.read_json(redis_conn.get('supplier'))
orders_df = pd.read_json(redis_conn.get('orders'))

# Prepare the data for merge (join) operation
part_df.rename(columns={'P_PARTKEY': 'L_PARTKEY'}, inplace=True)
supplier_df.rename(columns={'S_SUPPKEY': 'L_SUPPKEY', 'S_NATIONKEY': 'N_NATIONKEY'}, inplace=True)
orders_df.rename(columns={'O_ORDERKEY': 'L_ORDERKEY'}, inplace=True)

# Merge the dataframes to simulate the SQL joins
merged_df = (lineitem_df
             .merge(partsupp_df, on=['L_PARTKEY', 'L_SUPPKEY'])
             .merge(part_df[["L_PARTKEY", "P_NAME"]], on='L_PARTKEY')
             .merge(supplier_df, on='L_SUPPKEY')
             .merge(orders_df, on='L_ORDERKEY')
             .merge(nation_df, on='N_NATIONKEY'))

# Calculate the amount
merged_df['AMOUNT'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT']) - merged_df['PS_SUPPLYCOST'] * merged_df['L_QUANTITY']
merged_df['O_YEAR'] = pd.to_datetime(merged_df['O_ORDERDATE']).dt.year

# Group by nation and year, then sum the profit
result_df = (merged_df.groupby(['N_NAME', 'O_YEAR'])
             .agg(SUM_PROFIT=('AMOUNT', 'sum'))
             .reset_index()
             .sort_values(by=['N_NAME', 'O_YEAR'], ascending=[True, False]))

# Write the results to 'query_output.csv'
result_df.to_csv('query_output.csv', index=False)
