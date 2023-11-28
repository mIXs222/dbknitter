import pymysql
import pymongo
import pandas as pd
from pandas.io.json import json_normalize
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# MongoDB connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Redis connection
redis_conn = DirectRedis(host='redis', port=6379)

# Pull data from MySQL
def get_mysql_data():
    with mysql_conn.cursor() as cursor:
        cursor.execute("""
        SELECT s.S_SUPPKEY, s.S_NAME, s.S_ADDRESS, s.S_PHONE, s.S_ACCTBAL, s.S_COMMENT, n.N_NAME
        FROM supplier AS s
        JOIN nation AS n ON s.S_NATIONKEY = n.N_NATIONKEY
        JOIN region AS r ON n.N_REGIONKEY = r.R_REGIONKEY
        WHERE r.R_NAME = 'EUROPE'
        """)
        supplier_data = cursor.fetchall()
    supplier_df = pd.DataFrame(supplier_data, columns=[
        'S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT', 'N_NAME'])
    return supplier_df

# Pull data from MongoDB
def get_mongodb_data():
    region_data = list(mongodb_db.region.find({'R_NAME': 'EUROPE'}, {'_id': 0}))
    region_df = json_normalize(region_data)
    partsupp_data = list(mongodb_db.partsupp.find({}, {'_id': 0}))
    partsupp_df = json_normalize(partsupp_data)
    return partsupp_df

# Pull data from Redis
def get_redis_data():
    part_data = redis_conn.get('part')
    part_df = pd.read_json(part_data, orient='index')
    return part_df

supplier_df = get_mysql_data()
partsupp_df = get_mongodb_data()
part_df = get_redis_data()

# Filter part data
filtered_part_df = part_df[(part_df['P_SIZE'] == 15) & (part_df['P_TYPE'].str.contains('BRASS'))]

# Merge dataframes
merged_df = partsupp_df.merge(filtered_part_df, left_on='PS_PARTKEY', right_on='P_PARTKEY')\
                       .merge(supplier_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Final selection and sorting
result_df = merged_df[['S_ACCTBAL', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT',
                      'P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_SIZE', 'N_NAME']]\
             .sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

# Write results to CSV
result_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongodb_client.close()
