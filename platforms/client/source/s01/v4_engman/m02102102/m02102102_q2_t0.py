import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis = DirectRedis(host='redis', port=6379, db=0)

# Query to get suppliers and nations from MySQL
sql_query = """
SELECT s.S_SUPPKEY, s.S_NAME, s.S_ACCTBAL, s.S_ADDRESS, s.S_PHONE, s.S_COMMENT, n.N_NAME
FROM supplier s
JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
WHERE n.N_REGIONKEY = 
    (SELECT r.R_REGIONKEY FROM region r WHERE r.R_NAME = 'EUROPE')
"""

# Execute the SQL query
with mysql_conn.cursor() as cursor:
    cursor.execute(sql_query)
    suppliers_nations = cursor.fetchall()

# Convert to DataFrame
df_suppliers_nations = pd.DataFrame(suppliers_nations, columns=['S_SUPPKEY', 'S_NAME', 'S_ACCTBAL', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT', 'N_NAME'])

# Query to get parts from MongoDB
mongodb_parts = mongo_db['part'].find({'P_TYPE': 'BRASS', 'P_SIZE': 15}, {'P_PARTKEY': 1, 'P_MFGR': 1})
df_parts = pd.DataFrame(list(mongodb_parts))

# Query to get partsupp from Redis
partsupp_df = pd.read_json(redis.get('partsupp'), orient='records')

# Merge the frames to filter relevant supplies
df_merged = partsupp_df.merge(df_parts, left_on='PS_PARTKEY', right_on='P_PARTKEY')
df_merged = df_merged.merge(df_suppliers_nations, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Filter for minimum PS_SUPPLYCOST for each part
df_min_cost = df_merged.groupby('P_PARTKEY').apply(lambda x: x.loc[x.PS_SUPPLYCOST.idxmin()])

# Sort by S_ACCTBAL in descending, N_NAME, S_NAME, P_PARTKEY in ascending order
df_result = df_min_cost.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

# Select and rename the columns accordingly
df_output = df_result[['N_NAME', 'P_MFGR', 'P_PARTKEY', 'S_ACCTBAL', 'S_ADDRESS', 'S_COMMENT', 'S_NAME', 'S_PHONE']]

# Write to CSV
df_output.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongo_client.close()
