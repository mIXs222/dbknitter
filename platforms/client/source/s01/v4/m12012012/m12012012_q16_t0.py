import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
supplier_collection = mongodb_db['supplier']

# Redis connection
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve 'part' table from MySQL
mysql_query = """
SELECT P_PARTKEY, P_BRAND, P_TYPE, P_SIZE 
FROM part 
WHERE P_BRAND <> 'Brand#45' 
AND P_TYPE NOT LIKE 'MEDIUM POLISHED%' 
AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9);
"""
mysql_cursor.execute(mysql_query)
part_df = pd.DataFrame(mysql_cursor.fetchall(), columns=['P_PARTKEY', 'P_BRAND', 'P_TYPE', 'P_SIZE'])

# Retrieve 'supplier' table from MongoDB
supplier_query = {"S_COMMENT": {"$not": {"$regex": ".*Customer.*Complaints.*"}}}
supplier_df = pd.DataFrame(list(supplier_collection.find(supplier_query, {'_id': 0, 'S_SUPPKEY': 1})))
supplier_df.columns = ['PS_SUPPKEY']

# Retrieve 'partsupp' table from Redis
partsupp_df = pd.read_msgpack(redis_client.get('partsupp'))

# Merge 'part' with 'partsupp' within result from MySQL and Redis
merged_df = pd.merge(part_df, partsupp_df, how='inner', left_on='P_PARTKEY', right_on='PS_PARTKEY')

# Exclude suppliers with customer complaints
merged_df = merged_df[merged_df['PS_SUPPKEY'].isin(supplier_df['PS_SUPPKEY'])]

# Perform group by and count distinct suppliers
result_df = merged_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])['PS_SUPPKEY'].nunique().reset_index(name='SUPPLIER_CNT')

# Sort the results as per the requirement
result_df = result_df.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Write to CSV
result_df.to_csv('query_output.csv', index=False)

# Close the connections
mysql_conn.close()
mongodb_client.close()
