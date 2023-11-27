import pandas as pd
import pymysql
from pymongo import MongoClient
import redis
import direct_redis

# Define the connection parameters for MySQL
mysql_conn_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch'
}

# Define the connection parameters for MongoDB
mongo_conn_params = {
    'host': 'mongodb',
    'port': 27017,
    'db': 'tpch'
}

# Define the connection parameters for Redis
redis_conn_params = {
    'host': 'redis',
    'port': 6379,
    'db': 0
}

# Connect to MySQL
mysql_conn = pymysql.connect(**mysql_conn_params)
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = MongoClient(**mongo_conn_params)
mongo_db = mongo_client[mongo_conn_params['db']]

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host=redis_conn_params['host'], port=redis_conn_params['port'], db=redis_conn_params['db'])

# Fetch data from MySQL db (region and partsupp)
mysql_cursor.execute("SELECT * FROM region WHERE R_NAME = 'EUROPE'")
regions = pd.DataFrame(mysql_cursor.fetchall(), columns=['R_REGIONKEY', 'R_NAME', 'R_COMMENT'])

mysql_cursor.execute("SELECT * FROM partsupp")
partsupp = pd.DataFrame(mysql_cursor.fetchall(), columns=['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT'])

# Fetch data from MongoDB (part)
part_docs = mongo_db.part.find({'P_SIZE': 15, 'P_TYPE': {'$regex': 'BRASS'}})
parts = pd.DataFrame(list(part_docs))

# Fetch data from Redis (nation and supplier)
nation_data = eval(redis_conn.get('nation'))
supplier_data = eval(redis_conn.get('supplier'))

nations = pd.DataFrame(nation_data)
suppliers = pd.DataFrame(supplier_data)

# Merge the dataframes
merged = pd.merge(parts, partsupp, left_on='P_PARTKEY', right_on='PS_PARTKEY')
merged = pd.merge(merged, suppliers, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
merged = pd.merge(merged, nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
merged = pd.merge(merged, regions, left_on='N_REGIONKEY', right_on='R_REGIONKEY')

# Finding the minimal PS_SUPPLYCOST for the given conditions
min_cost = merged.loc[merged['R_NAME'] == 'EUROPE', 'PS_SUPPLYCOST'].min()
merged = merged[merged['PS_SUPPLYCOST'] == min_cost]

# Select the required columns
final_data = merged[['S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT']]

# Ordering the result
final_data.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True], inplace=True)

# Write to CSV
final_data.to_csv('query_output.csv', index=False)

# Close all the connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
redis_conn.close()
