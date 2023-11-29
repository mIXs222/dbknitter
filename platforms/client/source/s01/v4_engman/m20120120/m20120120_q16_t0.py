import pymysql
import pymongo
import pandas as pd
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Query for MySQL (partsupp)
mysql_query = """
    SELECT
        PS_SUPPKEY
    FROM
        partsupp
    WHERE
        PS_PARTKEY != 45
"""
mysql_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Query for MongoDB (part)
mongo_query = {
    '$and': [
        {'P_SIZE': {'$in': [49, 14, 23, 45, 19, 3, 36, 9]}},
        {'P_TYPE': {'$ne': 'MEDIUM POLISHED'}},
        {'P_BRAND': {'$ne': 'Brand#45'}}
    ]
}
mongo_df = pd.DataFrame(list(mongo_db.part.find(mongo_query, {'_id': 0, 'P_PARTKEY': 1})))

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Query for Redis (supplier)
supplier_df = pd.DataFrame(redis_conn.get('supplier'))

# Filtering suppliers without complaints
filtered_supplier_df = supplier_df[~supplier_df['S_COMMENT'].str.contains('Customer Complaints')]

# Combine the results
combined_df = pd.merge(
    mysql_df, 
    mongo_df, 
    left_on='PS_PARTKEY', 
    right_on='P_PARTKEY', 
)

final_result = combined_df[combined_df['PS_SUPPKEY'].isin(filtered_supplier_df['S_SUPPKEY'])]
final_result = final_result.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']).size().reset_index(name='SUPPLIER_COUNT')
final_result = final_result.sort_values(by=['SUPPLIER_COUNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Write result to CSV
final_result.to_csv('query_output.csv', index=False)
