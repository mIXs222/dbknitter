# query.py

import pymysql
import pymongo
import pandas as pd
import direct_redis

# MySQL Connection and Query
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_query = """
SELECT s.S_SUPPKEY, s.S_NAME
FROM supplier s
WHERE s.S_NAME NOT LIKE '%Better Business Bureau%'
"""
suppliers_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# MongoDB Connection and Query
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongodb = mongo_client['tpch']
parts_coll = mongodb['part']
mongo_query = {
    'P_SIZE': {'$in': [49, 14, 23, 45, 19, 3, 36, 9]},
    'P_TYPE': {'$ne': 'MEDIUM POLISHED'},
    'P_BRAND': {'$ne': 'Brand#45'}
}
parts_df = pd.DataFrame(list(parts_coll.find(mongo_query, {'_id': 0})))

# Redis Connection and Reading Data
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)
partsupp_df = pd.read_json(redis_conn.get('partsupp'))

# Processing the data
# Filter suppliers who can supply the parts based on parts' attributes.
parts_df['P_PARTKEY'] = parts_df['P_PARTKEY'].astype(str)
partsupp_df['PS_PARTKEY'] = partsupp_df['PS_PARTKEY'].astype(str)

# Merge the dataframes
merged_df = partsupp_df.merge(parts_df, left_on='PS_PARTKEY', right_on='P_PARTKEY')
merged_df = merged_df.merge(suppliers_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Group by the required fields and count unique suppliers
result_df = merged_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])['S_SUPPKEY'].nunique().reset_index()
result_df = result_df.rename(columns={'S_SUPPKEY': 'SUPPLIER_COUNT'})

# Sort the result as required
result_df.sort_values(by=['SUPPLIER_COUNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True], inplace=True)

# Write the result to CSV
result_df.to_csv('query_output.csv', index=False)
