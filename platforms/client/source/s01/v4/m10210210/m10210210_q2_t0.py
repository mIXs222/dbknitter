import pandas as pd
import pymysql
from pymongo import MongoClient
import direct_redis
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
nation = pd.DataFrame(list(mongo_db['nation'].find({})))
supplier = pd.DataFrame(list(mongo_db['supplier'].find({})))

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)
part = pd.read_msgpack(redis_conn.get('part'))

# Query part and partsupp from MySQL - tpch
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT * FROM region WHERE R_NAME = 'EUROPE'")
    region = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])

    cursor.execute("SELECT * FROM partsupp")
    partsupp = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])

mysql_conn.close()

# Filter the data
filtered_part = part[(part['P_SIZE'] == 15) & (part['P_TYPE'].str.contains('BRASS'))]
part_partsupp = filtered_part.merge(partsupp, left_on='P_PARTKEY', right_on='PS_PARTKEY')

# Join with MongoDB collections
joined_data = (part_partsupp
               .merge(supplier, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
               .merge(nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY'))

# Join with region
joined_data = joined_data.merge(region, left_on='N_REGIONKEY', right_on='R_REGIONKEY')

# Find the minimum PS_SUPPLYCOST for EUROPE
min_supply_cost_europe = (joined_data.loc[joined_data['R_NAME'] == 'EUROPE', 'PS_SUPPLYCOST'].min())

# Filter the joined data with minimum supply cost
result = joined_data[(joined_data['R_NAME'] == 'EUROPE') & 
                     (joined_data['PS_SUPPLYCOST'] == min_supply_cost_europe)]

# Select only required columns
output_cols = [
    'S_ACCTBAL',
    'S_NAME',
    'N_NAME',
    'P_PARTKEY',
    'P_MFGR',
    'S_ADDRESS',
    'S_PHONE',
    'S_COMMENT'
]

result = result[output_cols]

# Sort the results
result = result.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

# Write to CSV
result.to_csv('query_output.csv', index=False)
