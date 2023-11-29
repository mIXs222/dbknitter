import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL Database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Get partsupp table from MySQL
sql_query = """
SELECT PS_PARTKEY, PS_SUPPKEY
FROM partsupp
WHERE PS_PARTKEY NOT IN (
    SELECT P_PARTKEY
    FROM part
    WHERE P_BRAND = 'Brand#45' OR P_TYPE = 'MEDIUM POLISHED'
    OR P_SIZE NOT IN (49, 14, 23, 45, 19, 3, 36, 9)
)
"""
partsupp_df = pd.read_sql(sql_query, mysql_conn)

# MongoDB Database
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Retrieve complaint suppliers from MongoDB
supplier_cursor = mongo_db['supplier'].find(
    {'S_COMMENT': {'$not': {'$regex': '.*Customer.*Complaints.*'}}},
    {'_id': 0, 'S_SUPPKEY': 1}
)
supplier_df = pd.DataFrame(list(supplier_cursor))

mysql_conn.close()
mongo_client.close()

# Redis Database
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get part data based on partsupp part keys (i.e., PS_PARTKEY)
part_keys = partsupp_df['PS_PARTKEY'].unique()
part_data = []

for key in part_keys:
    key_data = redis_client.get(f'part:{key}')
    if key_data:
        part_data_dict = eval(key_data)
        part_data.append(part_data_dict)

part_df = pd.DataFrame(part_data)

# Merge dataframes
merged_df = pd.merge(part_df, partsupp_df, how='inner', left_on='P_PARTKEY', right_on='PS_PARTKEY')
merged_df = pd.merge(merged_df, supplier_df, how='inner', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Perform the final group and sort
result_df = merged_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])['S_SUPPKEY'].nunique().reset_index(name='SUPPLIER_COUNT')
result_df = result_df[result_df['SUPPLIER_COUNT'] > 0]  # Filter out rows with 0 counts
result_df.sort_values(['SUPPLIER_COUNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True], inplace=True)

# Save results to CSV
result_df.to_csv('query_output.csv', index=False)
