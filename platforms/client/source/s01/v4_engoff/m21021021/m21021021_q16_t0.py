import pymysql
import pymongo
import pandas as pd

# Connection to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

mysql_query = """
SELECT P_PARTKEY, P_NAME
FROM part
WHERE P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
AND P_TYPE NOT LIKE '%MEDIUM POLISHED%'
AND P_BRAND NOT LIKE 'Brand#45'
"""
mysql_cursor.execute(mysql_query)
part_candidates = pd.DataFrame(mysql_cursor.fetchall(), columns=['P_PARTKEY', 'P_NAME'])
mysql_cursor.close()
mysql_conn.close()

# Connection to MongoDB
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']
partsupp_collection = mongo_db['partsupp']
partsupp_docs = list(partsupp_collection.find({}, {'_id': 0, 'PS_PARTKEY': 1, 'PS_SUPPKEY': 1}))
partsupp_df = pd.DataFrame(partsupp_docs)

# Filter suppliers from partsupp that are in the part_candidates
qualified_partsupp = partsupp_df[partsupp_df['PS_PARTKEY'].isin(part_candidates['P_PARTKEY'])]

# Connection to Redis (assuming direct_redis.DirectRedis is available as specified)
import direct_redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

supplier_df = pd.DataFrame.from_records(
    [eval(redis_conn.get(f'supplier:{key}')) for key in redis_conn.keys('supplier:*')],
    columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT']
)

# Exclude suppliers with complaints from the Better Business Bureau
supplier_df = supplier_df[~supplier_df['S_COMMENT'].str.contains('Customer Complaints')]

# Join partsupp candidates with suppliers without complaints
result_df = qualified_partsupp.merge(supplier_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Group the results
final_df = result_df.groupby(['P_NAME']).size().reset_index(name='supplier_count')
final_df = final_df.sort_values(by=['supplier_count', 'P_NAME'], ascending=[False, True])

# Write the results to a CSV
final_df.to_csv('query_output.csv', index=False)
