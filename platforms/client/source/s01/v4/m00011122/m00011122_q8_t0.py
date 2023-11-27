# query.py
import pymysql
import pymongo
import pandas as pd
import direct_redis
from datetime import datetime

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = mongo_client["tpch"]

# Redis connection
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch data from MySQL
mysql_cursor.execute("SELECT * FROM part WHERE P_TYPE = 'SMALL PLATED COPPER'")
parts = pd.DataFrame(mysql_cursor.fetchall(), columns=['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'])

mysql_cursor.execute("SELECT * FROM nation")
nations = pd.DataFrame(mysql_cursor.fetchall(), columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])

mysql_cursor.execute("SELECT * FROM region WHERE R_NAME = 'ASIA'")
regions = pd.DataFrame(mysql_cursor.fetchall(), columns=['R_REGIONKEY', 'R_NAME', 'R_COMMENT'])

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Fetch data from MongoDB
suppliers = pd.DataFrame(list(mongodb.supplier.find()))
customers = pd.DataFrame(list(mongodb.customer.find()))

# Fetch data from Redis
orders_df = pd.read_json(redis_conn.get('orders'))
lineitem_df = pd.read_json(redis_conn.get('lineitem'))

# Merging dataframes
merged_df = (
    lineitem_df.merge(parts, left_on='L_PARTKEY', right_on='P_PARTKEY')
    .merge(suppliers, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    .merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    .merge(customers, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
    .merge(nations.rename(columns={'N_NAME': 'NATION', 'N_NATIONKEY': 'C_NATIONKEY'}), on='C_NATIONKEY')
    .merge(regions, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
)

# Apply the conditions from the query
merged_df = merged_df[
    (merged_df['O_ORDERDATE'] >= datetime(1995, 1, 1)) & 
    (merged_df['O_ORDERDATE'] <= datetime(1996, 12, 31)) &
    (merged_df['R_NAME'] == 'ASIA')
]

# Calculate O_YEAR, VOLUME, and MKT_SHARE
merged_df['O_YEAR'] = merged_df['O_ORDERDATE'].dt.year
merged_df['VOLUME'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
india_df = merged_df[merged_df['NATION'] == 'INDIA']

# Group by O_YEAR and calculate market share
result_df = merged_df.groupby('O_YEAR').agg({'VOLUME': 'sum'})
india_df = india_df.groupby('O_YEAR').agg({'VOLUME': 'sum'})
final_df = (india_df / result_df).reset_index().rename(columns={'VOLUME': 'MKT_SHARE'})

# Write to CSV
final_df.to_csv('query_output.csv', index=False)
