import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connecting to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT S_SUPPKEY, S_COMMENT FROM supplier")
    suppliers_data = cursor.fetchall()

# Filtering supplier data
suppliers_df = pd.DataFrame(suppliers_data, columns=['S_SUPPKEY', 'S_COMMENT'])
filtered_supplier_df = suppliers_df[~suppliers_df['S_COMMENT'].str.contains('Customer%Complaints%')]

# Connecting to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
part_collection = mongo_db["part"]

# Getting part data
part_query = {
    "P_BRAND": {"$ne": "Brand#45"},
    "P_TYPE": {"$not": {"$regex": "^MEDIUM POLISHED.*"}},
    "P_SIZE": {"$in": [49, 14, 23, 45, 19, 3, 36, 9]}
}
part_projection = {
    "P_PARTKEY": 1, "P_BRAND": 1, "P_TYPE": 1, "P_SIZE": 1
}
parts_data = list(part_collection.find(part_query, part_projection))
parts_df = pd.DataFrame(parts_data)

# Connecting to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)
partsupp_df = pd.read_json(redis_client.get('partsupp'))

# Joining datasets
result_df = parts_df.merge(partsupp_df, how='inner', left_on='P_PARTKEY', right_on='PS_PARTKEY')
result_df = result_df[~result_df['PS_SUPPKEY'].isin(filtered_supplier_df['S_SUPPKEY'])]

# Final query result
final_result = (
    result_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])
    .agg(SUPPLIER_CNT=('PS_SUPPKEY', 'nunique'))
    .reset_index()
    .sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])
)

# Writing output to a CSV file
final_result.to_csv('query_output.csv', index=False)
