import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL Connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# MongoDB Connection
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']

# Redis Connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Fetch data from MySQL
mysql_query = """
SELECT s.S_NATIONKEY, ps.PS_PARTKEY, ps.PS_SUPPKEY, ps.PS_SUPPLYCOST, ps.PS_AVAILQTY
FROM supplier s
JOIN partsupp ps ON s.S_SUPPKEY = ps.PS_SUPPKEY;
"""
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    supplier_partsupp = pd.DataFrame(cursor.fetchall(), columns=['S_NATIONKEY', 'PS_PARTKEY', 'PS_SUPPKEY', 'PS_SUPPLYCOST', 'PS_AVAILQTY'])

# Fetch data from MongoDB
mongo_parts = pd.DataFrame(list(mongo_db.part.find({"P_NAME": {"$regex": "dim"}})))
mongo_nation = pd.DataFrame(list(mongo_db.nation.find({})))

# Fetch data from Redis
orders_df = pd.read_msgpack(redis_conn.get('orders'))
lineitem_df = pd.read_msgpack(redis_conn.get('lineitem'))

# Close connections
mysql_conn.close()
mongo_client.close()

# Data processing
# Filter line items for parts with 'dim'
lineitem_df_dim = lineitem_df[lineitem_df['L_PARTKEY'].isin(mongo_parts['P_PARTKEY'])]

# Calculate profit
lineitem_df_dim['PROFIT'] = (lineitem_df_dim['L_EXTENDEDPRICE'] * (1 - lineitem_df_dim['L_DISCOUNT'])) - (supplier_partsupp['PS_SUPPLYCOST'] * lineitem_df_dim['L_QUANTITY'])

# Join tables to get the nation and orders information
joined_df = lineitem_df_dim.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY', how='inner')
joined_df = joined_df.merge(supplier_partsupp, left_on='L_SUPPKEY', right_on='PS_SUPPKEY')
joined_df = joined_df.merge(mongo_nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Group by Nation and Year
grouped_df = joined_df.groupby(['N_NAME', joined_df['O_ORDERDATE'].dt.year])['PROFIT'].sum().reset_index()

# Sort results
sorted_df = grouped_df.sort_values(by=['N_NAME', 'O_ORDERDATE'], ascending=[True, False])

# Write to CSV
sorted_df.to_csv('query_output.csv', index=False)
