import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection setup
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Querying partsupp table to retrieve needed information
mysql_query = """
SELECT PS_PARTKEY
FROM partsupp
"""
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    partsupp_data = cursor.fetchall()

# Transform partsupp tuple to dataframe
partsupp_df = pd.DataFrame(partsupp_data, columns=['PS_PARTKEY'])
mysql_conn.close()

# MongoDB connection setup
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Querying supplier table to retrieve needed information
supplier_df = pd.DataFrame(list(mongo_db.supplier.find(
    {'S_COMMENT': {'$not': {'$regex': '.*complaints.*'}}, 'S_SUPPKEY': {'$exists': True}}
)))

# Redis connection setup
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Load parts data from Redis
parts_data = redis_conn.get('part')
part_df = pd.read_json(parts_data)

# Filtering part data as per query
filtered_parts = part_df[
    (~part_df['P_TYPE'].str.contains('MEDIUM POLISHED')) &
    (part_df['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9])) &
    (part_df['P_BRAND'] != 'Brand#45')
]

# Merge to simulate join and filter parts that have suppliers
result_df = (
    filtered_parts.merge(partsupp_df, left_on='P_PARTKEY', right_on='PS_PARTKEY')
                  .merge(supplier_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
)

# Perform aggregation as per query instruction
output_df = result_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']).agg('count')

# Sort the result as instructed and write to csv
final_df = output_df.sort_values(['P_BRAND', 'P_SIZE', 'P_TYPE'])
final_df.to_csv('query_output.csv')
