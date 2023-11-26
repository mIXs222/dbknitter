import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from sqlalchemy import create_engine

# MySQL Connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# MongoDB Connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Redis Connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Load data from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT S_ACCTBAL, S_NAME, S_ADDRESS, S_PHONE, S_COMMENT, S_NATIONKEY, S_SUPPKEY FROM supplier")
    supplier_data = cursor.fetchall()

    cursor.execute("SELECT N_NAME, N_NATIONKEY, N_REGIONKEY FROM nation")
    nation_data = cursor.fetchall()

# Initialize pandas DataFrames from MySQL data
supplier_df = pd.DataFrame(supplier_data, columns=['S_ACCTBAL', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT', 'S_NATIONKEY', 'S_SUPPKEY'])
nation_df = pd.DataFrame(nation_data, columns=['N_NAME', 'N_NATIONKEY', 'N_REGIONKEY'])

# Load data from MongoDB
part_df = pd.DataFrame(list(mongo_db.part.find({'P_SIZE': 15, 'P_TYPE': {'$regex': '.*BRASS.*'}})))

# Load data from Redis
region_df = pd.read_json(redis_conn.get('region'))
partsupp_df = pd.read_json(redis_conn.get('partsupp'))

# Merge Redis data into pandas DataFrames
region_df.columns = ['R_REGIONKEY', 'R_NAME', 'R_COMMENT']
partsupp_df.columns = ['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT']

# Filtering regions for 'EUROPE' and finding minimum PS_SUPPLYCOST for each part
europe_regions = region_df[region_df['R_NAME'] == 'EUROPE']
min_cost_df = partsupp_df[partsupp_df['PS_SUPPLYCOST'] == partsupp_df.groupby('PS_PARTKEY')['PS_SUPPLYCOST'].transform('min')]
min_cost_df = min_cost_df.merge(europe_regions, left_on='R_REGIONKEY', right_on='R_REGIONKEY', how='inner')

# Merging tables together 
result = (
    part_df
    .merge(min_cost_df, left_on='P_PARTKEY', right_on='PS_PARTKEY')
    .merge(supplier_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
    .merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    .merge(europe_regions, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
)

# Selecting columns as per the original SQL query
result = result[['S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT']].copy()

# Sorting the result as per the original SQL query
result.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True], inplace=True)

# Write to CSV file
result.to_csv('query_output.csv', index=False)

# Close all connections
mysql_conn.close()
mongo_client.close()
