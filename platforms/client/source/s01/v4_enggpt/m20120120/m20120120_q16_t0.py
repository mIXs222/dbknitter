# query.py
import pymysql
from pymongo import MongoClient
import pandas as pd
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch')
# Execute query for partsupp from MySQL
partsupp_query = """
SELECT PS_PARTKEY, PS_SUPPKEY
FROM partsupp
"""
partsupp_df = pd.read_sql(partsupp_query, mysql_conn)
mysql_conn.close()

# Connect to MongoDB
mongo_client = MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']
part_collection = mongo_db['part']
# Query for part from MongoDB
part_query = {
    'P_PARTKEY': {'$in': list(partsupp_df['PS_PARTKEY'])},
    'P_BRAND': {'$ne': 'Brand#45'},
    'P_TYPE': {'$not': {'$regex': '^MEDIUM POLISHED'}},
    'P_SIZE': {'$in': [49, 14, 23, 45, 19, 3, 36, 9]}
}
part_cursor = part_collection.find(part_query, {'_id': 0})
part_df = pd.DataFrame(list(part_cursor))
mongo_client.close()

# Combine and process partsupp and part dataframes
combined_df = partsupp_df.merge(part_df, how='inner', left_on='PS_PARTKEY', right_on='P_PARTKEY')

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)
# Filter out suppliers with comments containing 'Customer Complaints'
supplier_df = pd.DataFrame()
for key in redis_conn.get('supplier'):
    supplier_info = pd.read_json(key)
    if 'Customer Complaints' not in supplier_info['S_COMMENT']:
        supplier_df = supplier_df.append(supplier_info, ignore_index=True)
redis_conn.connection_pool.disconnect()

# Merge combined_df with supplier_df using PS_SUPPKEY and S_SUPPKEY
results_df = combined_df.merge(supplier_df, how='inner', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Group by brand, type, and size and count distinct suppliers
grouped_df = results_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']).agg(SUPPLIER_CNT=('PS_SUPPKEY', 'nunique')).reset_index()

# Sort the results according to the specified condition
sorted_df = grouped_df.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Write the final result to query_output.csv
sorted_df.to_csv('query_output.csv', index=False)
