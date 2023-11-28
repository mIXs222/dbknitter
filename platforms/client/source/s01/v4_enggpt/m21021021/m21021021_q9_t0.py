import pymysql
import pymongo
import pandas as pd
from datetime import datetime
import direct_redis

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
redis_client = direct_redis.DirectRedis(host='redis', port=6379)

# Query MySQL for part data containing 'dim' in P_NAME 
part_sql = """
SELECT P_PARTKEY, P_NAME 
FROM part 
WHERE P_NAME LIKE '%dim%';
"""
part_df = pd.read_sql(part_sql, mysql_conn)
mysql_conn.close()

# Query MongoDB for partsupp data
partsupp_coll = mongo_db['partsupp']
partsupp_df = pd.DataFrame(list(partsupp_coll.find({"PS_PARTKEY": {"$in": part_df['P_PARTKEY'].tolist()}})))

# Query MongoDB for lineitem data
lineitem_coll = mongo_db['lineitem']
lineitem_df = pd.DataFrame(list(lineitem_coll.find()))

# Combine part and partsupp data
combined_df = pd.merge(part_df, partsupp_df, how='inner', left_on='P_PARTKEY', right_on='PS_PARTKEY')

# Combine with lineitem data
combined_df = pd.merge(combined_df, lineitem_df, how='inner', left_on='P_PARTKEY', right_on='L_PARTKEY')

# Query Redis for orders data, convert to pandas DataFrame
orders_df = pd.read_json(redis_client.get('orders'))

# Query Redis for nation data, convert to pandas DataFrame
nation_df = pd.read_json(redis_client.get('nation'))

# Combine with orders and nation data
combined_df = pd.merge(combined_df, orders_df, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
combined_df = pd.merge(combined_df, nation_df, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Calculate profit
combined_df['PROFIT'] = (combined_df['L_EXTENDEDPRICE'] * (1 - combined_df['L_DISCOUNT']) 
                         - (combined_df['L_QUANTITY'] * combined_df['PS_SUPPLYCOST']))

# Extract order year and group by nation and year
combined_df['YEAR'] = pd.to_datetime(combined_df['O_ORDERDATE']).dt.year
result_df = combined_df.groupby(['N_NAME', 'YEAR']).agg({'PROFIT': 'sum'}).reset_index()

# Sort results
result_df = result_df.sort_values(by=['N_NAME', 'YEAR'], ascending=[True, False])

# Write to CSV
result_df.to_csv('query_output.csv', index=False)
