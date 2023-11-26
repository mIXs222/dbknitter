# query_execution.py
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_conn = DirectRedis(port=6379, host='redis', db=0)

# Load data from MySQL
supplier_df = pd.read_sql("SELECT * FROM supplier", mysql_conn)
partsupp_df = pd.read_sql("SELECT * FROM partsupp", mysql_conn)

# Load data from MongoDB
nation_df = pd.DataFrame(list(mongo_db.nation.find()))
part_df = pd.DataFrame(list(mongo_db.part.find({ "P_NAME": { "$regex": ".*dim.*" } })))

# Load data from Redis
orders_df = pd.read_json(redis_conn.get('orders').decode('utf-8'))
lineitem_df = pd.read_json(redis_conn.get('lineitem').decode('utf-8'))

# Close connections
mysql_conn.close()
mongo_client.close()

# Format and merge data
lineitem_df['L_EXTENDEDPRICE'] = lineitem_df['L_EXTENDEDPRICE'].astype(float)
lineitem_df['L_DISCOUNT'] = lineitem_df['L_DISCOUNT'].astype(float)
lineitem_df['L_QUANTITY'] = lineitem_df['L_QUANTITY'].astype(float)
merged_df = lineitem_df.merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
merged_df = merged_df.merge(partsupp_df, left_on=['L_PARTKEY', 'L_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY'])
merged_df = merged_df.merge(part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')
merged_df = merged_df.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merged_df = merged_df.merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Calculate Profit
merged_df['O_YEAR'] = merged_df['O_ORDERDATE'].apply(lambda x: datetime.utcfromtimestamp(x).strftime('%Y'))
merged_df['AMOUNT'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT']) - merged_df['PS_SUPPLYCOST'] * merged_df['L_QUANTITY']

# Group by and aggregate
result_df = merged_df.groupby(['N_NAME', 'O_YEAR'])['AMOUNT'].sum().reset_index()
result_df.columns = ['NATION', 'O_YEAR', 'SUM_PROFIT']
result_df.sort_values(['NATION', 'O_YEAR'], ascending=[1, 0], inplace=True)

# Write to CSV
result_df.to_csv('query_output.csv', index=False)
