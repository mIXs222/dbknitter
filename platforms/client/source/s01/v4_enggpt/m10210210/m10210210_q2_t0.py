import pandas as pd
import pymysql
import pymongo
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Query the MySQL Database
mysql_query = """
SELECT r.R_NAME, ps.PS_PARTKEY, ps.PS_SUPPKEY, ps.PS_SUPPLYCOST
FROM region r
JOIN nation n ON r.R_REGIONKEY = n.N_REGIONKEY
JOIN supplier s ON n.N_NATIONKEY = s.S_NATIONKEY
JOIN partsupp ps ON s.S_SUPPKEY = ps.PS_SUPPKEY
WHERE r.R_NAME = 'EUROPE'
"""

mysql_df = pd.read_sql(mysql_query, mysql_conn)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Query the MongoDB Database
nation_df = pd.DataFrame(list(mongo_db['nation'].find({})))
supplier_df = pd.DataFrame(list(mongo_db['supplier'].find({})))

# Combine the MongoDB DataFrames
combined_supplier_df = supplier_df.merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
combined_supplier_df = combined_supplier_df.merge(mysql_df, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Query the Redis Database
part_df = pd.DataFrame(redis_conn.get('part'), columns=[
    'P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'
])

# Final DataFrame using criteria
final_df = combined_supplier_df.merge(part_df,
                                      left_on='PS_PARTKEY',
                                      right_on='P_PARTKEY')

final_df = final_df[
    (final_df['P_SIZE'] == 15) &
    (final_df['P_TYPE'].str.contains('BRASS')) &
    (final_df['R_NAME'] == 'EUROPE')
]

final_df['PS_SUPPLYCOST'] = final_df['PS_SUPPLYCOST'].astype(float)
final_df = final_df.sort_values(
    by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'],
    ascending=[False, True, True, True]
)

# Select desired columns
output_df = final_df[
    ['S_ACCTBAL', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT', 'P_PARTKEY', 'P_MFGR', 'P_SIZE']
]

# Write DataFrame to CSV
output_df.to_csv('query_output.csv', index=False)

# Close the connections
mysql_conn.close()
mongo_client.close()
