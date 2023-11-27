import pymysql
import pymongo
import pandas as pd
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# MySQL query to get suppliers without complaints
mysql_query = """
SELECT S_SUPPKEY
FROM supplier
WHERE S_COMMENT NOT LIKE '%Customer%Complaints%'
"""

# Execute MySQL query
supplier_data = pd.read_sql(mysql_query, mysql_conn)

# MongoDB query to get partsupp for the suppliers in the result
partsupp_data = pd.DataFrame(list(mongo_db.partsupp.find(
    {'PS_SUPPKEY': {'$in': supplier_data['S_SUPPKEY'].tolist()}},
    {'PS_PARTKEY': 1, '_id': 0}
)))

# Redis query to get parts with the given attributes
sizes = [49, 14, 23, 45, 19, 3, 36, 9]
parts_data = pd.DataFrame()
for size in sizes:
    data = pd.read_json(redis_conn.get(f'part:size:{size}'))
    parts_data = pd.concat([parts_data, data], ignore_index=True)

parts_data = parts_data[
    (parts_data['P_SIZE'].isin(sizes)) & 
    (parts_data['P_TYPE'] != 'MEDIUM POLISHED') & 
    (parts_data['P_BRAND'] != 'Brand#45')
]

# Combine the data to get the final result
final_data = partsupp_data.merge(parts_data, left_on='PS_PARTKEY', right_on='P_PARTKEY')
final_result = final_data.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])['PS_SUPPKEY'].nunique().reset_index()
final_result = final_result.rename(columns={'PS_SUPPKEY': 'SupplierCount'})
final_result = final_result.sort_values(by=['SupplierCount', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Output to CSV
final_result.to_csv('query_output.csv', index=False)
