import pandas as pd
import pymysql
import pymongo
import sys

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', 
                             database='tpch', 
                             user='root', 
                             password='my-secret-pw')
try:
    # Load nation and supplier tables from MySQL
    df_nation = pd.read_sql('SELECT * FROM nation', mysql_conn)
    df_supplier = pd.read_sql('SELECT * FROM supplier', mysql_conn)
finally:
    mysql_conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client['tpch']
try:
    # Load region and partsupp tables from MongoDB
    df_region = pd.DataFrame(list(mongo_db.region.find()))
    df_partsupp = pd.DataFrame(list(mongo_db.partsupp.find()))
finally:
    mongo_client.close()

# Filtering the region to be 'EUROPE'
df_region = df_region[df_region['R_NAME'] == 'EUROPE']

# Filtering the nation to be within the EUROPE region
df_nation = df_nation[df_nation['N_REGIONKEY'].isin(df_region['R_REGIONKEY'])]

# Merging the nation with the suppliers in EUROPE region
df_supplier = pd.merge(df_supplier, df_nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Now, load the parts table from Redis using direct_redis
import direct_redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)
try:
    # Get part data from Redis
    df_part = pd.read_json(redis_conn.get('part'))
finally:
    redis_conn.close()

# Filtering to select only 'BRASS' parts of size '15'
df_part = df_part[(df_part['P_TYPE'] == 'BRASS') & (df_part['P_SIZE'] == 15)]

# Merging partsupp with the parts (BRASS and size 15) to get the supply details
df_partsupp = pd.merge(df_partsupp, df_part, left_on='PS_PARTKEY', right_on='P_PARTKEY')

# Finding the minimum cost for each part from the partsupp table
df_min_cost = df_partsupp.groupby('PS_PARTKEY')['PS_SUPPLYCOST'].min().reset_index()
df_partsupp = pd.merge(df_partsupp, df_min_cost, on=['PS_PARTKEY', 'PS_SUPPLYCOST'])

# Merge supplier and partsupp on supplier key and select only suppliers from the EUROPE region
df_result = pd.merge(df_supplier, df_partsupp, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')

# Sorting the final result as specified by the query
df_result.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True], inplace=True)

# Selecting required columns
df_final = df_result[['N_NAME', 'P_MFGR', 'P_PARTKEY', 'S_ACCTBAL', 'S_ADDRESS', 'S_COMMENT', 'S_NAME', 'S_PHONE']]

# Write the result to CSV
df_final.to_csv('query_output.csv', index=False)
