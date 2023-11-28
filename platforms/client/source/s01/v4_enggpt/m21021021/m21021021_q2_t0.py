import pandas as pd
import pymysql
from pymongo import MongoClient
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Get 'part' table from MySQL
part_query = """
SELECT P_PARTKEY, P_MFGR, P_SIZE 
FROM part
WHERE P_SIZE = 15 AND P_TYPE LIKE '%BRASS'
"""
parts_df = pd.read_sql(part_query, mysql_conn)
mysql_conn.close()

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Get 'region' and 'partsupp' collections from MongoDB
region_df = pd.DataFrame(list(mongo_db.region.find({'R_NAME': 'EUROPE'})))
partsupp_df = pd.DataFrame(list(mongo_db.partsupp.find()))

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get 'nation' and 'supplier' tables from Redis
nation_df = pd.read_json(redis_conn.get('nation'))
supplier_df = pd.read_json(redis_conn.get('supplier'))

# Perform the join and selection as per the given query
region_nation_df = pd.merge(nation_df, region_df, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
supplier_nation_df = pd.merge(supplier_df, region_nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
partsupp_supplier_df = pd.merge(partsupp_df, supplier_nation_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
final_df = pd.merge(partsupp_supplier_df, parts_df, left_on='PS_PARTKEY', right_on='P_PARTKEY')

# Selecting minimum PS_SUPPLYCOST within the 'EUROPE' region
final_df = final_df.loc[final_df.groupby('PS_PARTKEY')['PS_SUPPLYCOST'].idxmin()]

# Select relevant columns and order the results
final_df = final_df[['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY', 'P_MFGR', 'P_SIZE', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT']]
final_df = final_df.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

# Write the results to a CSV file
final_df.to_csv('query_output.csv', index=False)
