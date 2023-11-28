# import necessary libraries
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime
import csv

# Create connection to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch')

# Create connection to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Create connection to Redis
redis = DirectRedis(host='redis', port=6379, db=0)

# Query MySQL for parts and nations
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT * FROM part WHERE P_NAME LIKE '%dim%';")
    part_dim = pd.DataFrame(cursor.fetchall(), columns=['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'])
    
    cursor.execute("SELECT * FROM nation;")
    nations = pd.DataFrame(cursor.fetchall(), columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])

# Query MongoDB for suppliers and partsupp
suppliers = pd.DataFrame(list(mongodb['supplier'].find({})))
partsupp = pd.DataFrame(list(mongodb['partsupp'].find({})))

# Query Redis for orders and lineitems
orders = pd.read_json(redis.get('orders'))
lineitem = pd.read_json(redis.get('lineitem'))

# Merge and calculate profits
partsupp = partsupp.rename(columns={'PS_PARTKEY': 'P_PARTKEY', 'PS_SUPPKEY': 'S_SUPPKEY'})
lineitem = lineitem.merge(part_dim, on='P_PARTKEY')
lineitem = lineitem.merge(partsupp, on=['P_PARTKEY', 'S_SUPPKEY'])
lineitem = lineitem.merge(suppliers, on='S_SUPPKEY')
lineitem['PROFIT'] = (lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT']) - (lineitem['PS_SUPPLYCOST'] * lineitem['L_QUANTITY']))

# Merge lineitem with orders and nation
result = lineitem.merge(orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
result = result.merge(nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
result['YEAR'] = result['O_ORDERDATE'].apply(lambda d: datetime.strptime(d, '%Y-%m-%d').year)
result = result[['N_NAME', 'YEAR', 'PROFIT']]
result = result.groupby(['N_NAME', 'YEAR']).sum().reset_index()

# Sorting the results
result.sort_values(by=['N_NAME', 'YEAR'], ascending=[True, False], inplace=True)

# Writing results to csv
result.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongo_client.close()
redis.close()
