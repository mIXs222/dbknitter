import pymongo
import pymysql
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and query
mysql_conn = pymysql.connect(
    host='mysql', user='root', password='my-secret-pw', database='tpch'
)

mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute("""
    SELECT P_PARTKEY, P_MFGR
    FROM part
    WHERE P_TYPE = 'BRASS' AND P_SIZE = 15
""")
mysql_parts = pd.DataFrame(mysql_cursor.fetchall(), columns=['P_PARTKEY', 'P_MFGR'])
mysql_cursor.close()
mysql_conn.close()

# MongoDB connection and query
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

mongo_nations = pd.DataFrame(list(mongo_db['nation'].find(
    {'N_NAME': {'$in': ['EUROPE']}}
)))
mongo_suppliers = pd.DataFrame(list(mongo_db['supplier'].find()))

# Merging MongoDB collections
mongo_merge = pd.merge(mongo_suppliers, mongo_nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Redis connection
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Fetching data from Redis
redis_partsupps = pd.read_json(redis_client.get('partsupp'))
redis_regions = pd.read_json(redis_client.get('region'))

# Filtering for EUROPE in the region
europe_regionkey = redis_regions[redis_regions['R_NAME'] == 'EUROPE']['R_REGIONKEY'].iloc[0]
europe_nations = mongo_nations[mongo_nations['N_REGIONKEY'] == europe_regionkey]

# Merging part suppliers with Europe nations and parts with BRASS type and size 15
europe_suppliers = mongo_merge[mongo_merge['N_NATIONKEY'].isin(europe_nations['N_NATIONKEY'])]
part_suppliers = pd.merge(europe_suppliers, redis_partsupps, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')
final_merge = pd.merge(part_suppliers, mysql_parts, left_on='PS_PARTKEY', right_on='P_PARTKEY')

# Sort and filter minimum cost suppliers
final_merge.sort_values(by=['PS_SUPPLYCOST', 'S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], 
                        ascending=[True, False, True, True, True], inplace=True)

final_merge['min_cost'] = final_merge.groupby('P_PARTKEY')['PS_SUPPLYCOST'].transform('min')
min_cost_suppliers = final_merge[final_merge['PS_SUPPLYCOST'] == final_merge['min_cost']]

# Selecting the relevant columns for the output
output_columns = [
    'S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR',
    'S_ADDRESS', 'S_PHONE', 'S_COMMENT'
]
result = min_cost_suppliers[output_columns]

# Writing the result to a CSV file
result.to_csv('query_output.csv', index=False)
