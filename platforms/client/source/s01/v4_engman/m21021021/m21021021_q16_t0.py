import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and query
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_query = """
    SELECT P_NAME, P_MFGR, P_TYPE, P_SIZE, COUNT(DISTINCT PS_SUPPKEY) AS SUPPLIER_COUNT
    FROM part
    LEFT JOIN partsupp ON part.P_PARTKEY = partsupp.PS_PARTKEY
    WHERE P_BRAND <> 'Brand#45'
    AND P_TYPE NOT LIKE 'MEDIUM POLISHED%'
    AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
    GROUP BY P_NAME, P_MFGR, P_TYPE, P_SIZE
    ORDER BY SUPPLIER_COUNT DESC, P_BRAND, P_TYPE, P_SIZE;
"""
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    parts_result = cursor.fetchall()
    df_mysql = pd.DataFrame(parts_result, columns=['P_NAME', 'P_MFGR', 'P_TYPE', 'P_SIZE', 'SUPPLIER_COUNT'])

mysql_conn.close()

# MongoDB connection and query
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_collection = mongo_db['partsupp']
partsupp_query = {
    "PS_PARTKEY": {"$not": {"$eq": 45}},
    "PS_SUPPKEY": {"$not": {"$regex": ".*[cC]omplaint.*"}},
    "PS_AVAILQTY": {"$gt": 0}
}

partsupp_result = mongo_collection.find(partsupp_query)
df_mongo = pd.DataFrame(list(partsupp_result))  # Auto-extract field names

mongo_client.close()

# Redis connection and query
redis_conn = DirectRedis(host='redis', port=6379, db=0)
supplier_data = redis_conn.get('supplier')
df_redis = pd.read_csv(pd.compat.StringIO(supplier_data))

# Merge the results from different databases
df_merged = pd.merge(df_mysql, df_mongo, left_on='P_PARTKEY', right_on='PS_PARTKEY', how='left')
df_final = pd.merge(df_merged, df_redis, left_on='PS_SUPPKEY', right_on='S_SUPPKEY', how='left')

# Filter out 'complaint' suppliers
df_final = df_final[~df_final['S_COMMENT'].str.contains('complaint', na=False)]

# Select and rename final columns
df_output = df_final[['P_NAME', 'P_MFGR', 'P_TYPE', 'P_SIZE', 'SUPPLIER_COUNT']].copy()
df_output.columns = ['Part Name', 'Manufacturer', 'Type', 'Size', 'Supplier Count']

# Write the query output to a csv file
df_output.to_csv('query_output.csv', index=False)
