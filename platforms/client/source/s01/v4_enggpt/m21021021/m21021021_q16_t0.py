# Python code: analyze_parts_suppliers.py

import pandas as pd
import pymysql
import pymongo
import direct_redis

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Execute MySQL query
mysql_query = """
SELECT P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT
FROM part
WHERE P_BRAND <> 'Brand#45' 
AND P_TYPE NOT LIKE 'MEDIUM POLISHED%%'
AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9);
"""
mysql_cursor.execute(mysql_query)

# Fetch data from MySQL
part_data = pd.DataFrame(mysql_cursor.fetchall(), columns=['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'])

mysql_conn.close()

# MongoDB connection and query
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
parts_collection = mongo_db['partsupp']

# Fetch data from MongoDB
partsupp_data = pd.DataFrame(list(parts_collection.find({}, {'_id': 0})))

# Redis connection
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get supplier data from Redis
supplier_data = pd.DataFrame(eval(redis_client.get('supplier')))

# Merge data frames
merged_data = pd.merge(part_data, partsupp_data, how='inner', left_on='P_PARTKEY', right_on='PS_PARTKEY')

# Remove suppliers with unwanted comments
merged_data = merged_data[~merged_data['PS_COMMENT'].str.contains('Customer Complaints')]

# Calculate count of distinct suppliers for each combination of brand, type, and size
supplier_count = merged_data.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])['PS_SUPPKEY'].nunique().reset_index(name='SUPPLIER_CNT')

# Sort results
supplier_count.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True], inplace=True)

# Write to CSV
supplier_count.to_csv('query_output.csv', index=False)
