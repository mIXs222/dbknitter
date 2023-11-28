# query.py

import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Function to establish connection to MySQL
def connect_mysql(host, user, password, db):
    return pymysql.connect(host=host, user=user, password=password, db=db)

# Function to establish connection to MongoDB
def connect_mongodb(host, port, db):
    client = pymongo.MongoClient(host=host, port=port)
    return client[db]

# Function to connect to Redis
def connect_redis(host, port, db):
    return DirectRedis(host=host, port=port, db=db)

# MySQL connection
mysql_conn = connect_mysql('mysql', 'root', 'my-secret-pw', 'tpch')

# Fetching data from mysql tables
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT * FROM nation")
    nation = pd.DataFrame(cursor.fetchall(), columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])

    cursor.execute("SELECT * FROM supplier")
    supplier = pd.DataFrame(cursor.fetchall(), columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])

    cursor.execute("SELECT * FROM orders WHERE O_ORDERDATE LIKE '%%%dim%%'")
    orders = pd.DataFrame(cursor.fetchall(), columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT'])

# MongoDB connection
mongodb_conn = connect_mongodb('mongodb', 27017, 'tpch')

# Fetching data from the MongoDB part collection
part = pd.DataFrame(list(mongodb_conn['part'].find({'P_NAME': {'$regex': '.*dim.*'}})))

# Redis connection
redis_conn = connect_redis('redis', 6379, 0)

# Fetching data from Redis
partsupp = pd.read_json(redis_conn.get('partsupp'))
lineitem = pd.read_json(redis_conn.get('lineitem'))

# Data transformation and analysis
# Join part with partsupp on P_PARTKEY = PS_PARTKEY
part_partsupp = pd.merge(part, partsupp, how='inner', left_on='P_PARTKEY', right_on='PS_PARTKEY')

# Join the result with lineitem on partkey and suppkey
part_lineitem = pd.merge(part_partsupp, lineitem, how='inner', left_on=['P_PARTKEY', 'PS_SUPPKEY'], right_on=['L_PARTKEY', 'L_SUPPKEY'])

# Join result with orders on orderkey and filter for 'dim'
profit_analysis = pd.merge(part_lineitem, orders, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Join result with supplier on suppkey
profit_analysis = pd.merge(profit_analysis, supplier, how='inner', left_on='S_SUPPKEY', right_on='S_SUPPKEY')

# Join result with nation on nationkey
profit_analysis = pd.merge(profit_analysis, nation, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Calculate profit
profit_analysis['PROFIT'] = (profit_analysis['L_EXTENDEDPRICE'] * (1 - profit_analysis['L_DISCOUNT'])) - (profit_analysis['PS_SUPPLYCOST'] * profit_analysis['L_QUANTITY'])

# Extract year from O_ORDERDATE
profit_analysis['YEAR'] = pd.to_datetime(profit_analysis['O_ORDERDATE']).dt.year

# Group by nation and year
result = profit_analysis.groupby(['N_NAME', 'YEAR']).agg({'PROFIT': 'sum'}).reset_index()

# Sort the result and write to CSV
result.sort_values(by=['N_NAME', 'YEAR'], ascending=[True, False]).to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
