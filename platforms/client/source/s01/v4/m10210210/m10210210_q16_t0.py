import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
)

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Redis connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Load data from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY FROM partsupp")
    partsupp_data = cursor.fetchall()
partsupp_df = pd.DataFrame(partsupp_data, columns=['P_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY'])

# Load data from MongoDB
supplier_data = list(mongo_db['supplier'].find({}, {'_id': 0, 'S_SUPPKEY': 1, 'S_COMMENT': 1}))
supplier_df = pd.DataFrame(supplier_data)

# Load data from Redis
part_df = pd.read_json(redis_conn.get('part'), orient='records')

# Filter out suppliers with complaints
filtered_supplier_df = supplier_df[~supplier_df['S_COMMENT'].str.contains('Customer.*Complaints')]

# Merge and perform the query
merged_df = partsupp_df.merge(part_df, how='inner', left_on='PS_PARTKEY', right_on='P_PARTKEY')
merged_df = merged_df[merged_df['P_BRAND'] != 'Brand#45']
merged_df = merged_df[~merged_df['P_TYPE'].str.startswith('MEDIUM POLISHED')]
merged_df = merged_df[merged_df['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9])]
filtered_supplier_keys = filtered_supplier_df['S_SUPPKEY']
merged_df = merged_df[~merged_df['PS_SUPPKEY'].isin(filtered_supplier_keys)]

# Group by and aggregate the count of suppliers
result_df = merged_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']) \
                     .agg(SUPPLIER_CNT=('PS_SUPPKEY', 'nunique')) \
                     .reset_index()

# Sort the result
result_df.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True], inplace=True)

# Write to CSV
result_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongo_client.close()
redis_conn.close()
