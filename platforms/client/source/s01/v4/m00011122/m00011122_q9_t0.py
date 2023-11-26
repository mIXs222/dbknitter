# query.py
import pandas as pd
import pymysql
from pymongo import MongoClient
import direct_redis

# MySQL Connection Information
mysql_conn_info = {
    "host": "mysql",
    "user": "root",
    "password": "my-secret-pw",
    "db": "tpch"
}

# MongoDB Connection Information
mongodb_conn_info = {
    "host": "mongodb",
    "port": 27017,
}

# Redis Connection Information
redis_conn_info = {
    "host": "redis",
    "port": 6379,
    "db": 0
}

# Connect to MySQL
mysql_connection = pymysql.connect(**mysql_conn_info)

# Connect to MongoDB
mongo_client = MongoClient(**mongodb_conn_info)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_client = direct_redis.DirectRedis(**redis_conn_info)

# Get MySQL data
with mysql_connection.cursor() as cursor:
    cursor.execute("SELECT * FROM nation")
    nations_df = pd.DataFrame(cursor.fetchall(), columns=["N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT"])

    cursor.execute("SELECT * FROM part WHERE P_NAME LIKE '%dim%'")
    parts_df = pd.DataFrame(cursor.fetchall(), columns=["P_PARTKEY", "P_NAME", "P_MFGR", "P_BRAND", "P_TYPE", "P_SIZE", "P_CONTAINER", "P_RETAILPRICE", "P_COMMENT"])

# Get MongoDB data
suppliers_df = pd.DataFrame(list(mongo_db.supplier.find()), columns=["_id", "S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_NATIONKEY", "S_PHONE", "S_ACCTBAL", "S_COMMENT"]).drop(columns=["_id"])
partsupp_df = pd.DataFrame(list(mongo_db.partsupp.find()), columns=["_id", "PS_PARTKEY", "PS_SUPPKEY", "PS_AVAILQTY", "PS_SUPPLYCOST", "PS_COMMENT"]).drop(columns=["_id"])

# Get Redis data
orders_df = pd.read_msgpack(redis_client.get('orders'))
lineitem_df = pd.read_msgpack(redis_client.get('lineitem'))

# Close connections
mysql_connection.close()

# Data merging and calculations
merged_df = pd.merge(lineitem_df, orders_df, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merged_df = pd.merge(merged_df, nations_df, how='inner', left_on='L_SUPPKEY', right_on='N_NATIONKEY')
merged_df = pd.merge(merged_df, parts_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')
merged_df = pd.merge(merged_df, suppliers_df, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')
merged_df = pd.merge(merged_df, partsupp_df, how='inner', left_on=['L_SUPPKEY', 'L_PARTKEY'], right_on=['PS_SUPPKEY', 'PS_PARTKEY'])

merged_df['O_YEAR'] = merged_df['O_ORDERDATE'].str[:4]
merged_df['AMOUNT'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT']) - merged_df['PS_SUPPLYCOST'] * merged_df['L_QUANTITY']

# Group by and sort
result_df = (
    merged_df.groupby(['N_NAME', 'O_YEAR'])
    .agg(SUM_PROFIT=pd.NamedAgg(column='AMOUNT', aggfunc='sum'))
    .reset_index()
    .sort_values(by=['N_NAME', 'O_YEAR'], ascending=[True, False])
)

# Write to CSV
result_df.to_csv("query_output.csv", index=False)
