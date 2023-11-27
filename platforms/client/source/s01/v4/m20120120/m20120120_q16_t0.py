import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Select from partsupp table in MySQL
partsupp_query = """
SELECT PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT
FROM partsupp
"""
partsupp_df = pd.read_sql(partsupp_query, mysql_conn)
mysql_conn.close()

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Select from part collection in MongoDB
part_df = pd.DataFrame(list(mongo_db.part.find()))
mongo_client.close()

# Redis connection
redis_host = 'redis'
redis_port = 6379
redis_db = DirectRedis(host=redis_host, port=redis_port)

# Get supplier data from Redis and create DataFrame
supplier_str_df = redis_db.get("supplier")
supplier_df = pd.read_csv(pd.compat.StringIO(supplier_str_df))

# Filter out suppliers with '%Customer%Complaints%' in comment
filtered_supplier_df = supplier_df[~supplier_df['S_COMMENT'].str.contains('Customer.*Complaints')]

# Join and Query
result = pd.merge(part_df, partsupp_df, left_on='P_PARTKEY', right_on='PS_PARTKEY')
result = result[
    (result['P_BRAND'] != 'Brand#45') &
    (~result['P_TYPE'].str.startswith('MEDIUM POLISHED')) &
    (result['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9]))
]

# Filter out parts that their supplier is in the complaint list
result = result[~result['PS_SUPPKEY'].isin(filtered_supplier_df['S_SUPPKEY'])]

# Group by and count distinct PS_SUPPKEY
final_result = (result.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])
                .agg(SUPPLIER_CNT=('PS_SUPPKEY', 'nunique'))
                .reset_index()
                .sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True]))

# Write the final result to a CSV
final_result.to_csv('query_output.csv', index=False)
