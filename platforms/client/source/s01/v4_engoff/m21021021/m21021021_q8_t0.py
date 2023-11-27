import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and query
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
with mysql_conn.cursor() as cursor:
    query_part = "SELECT P_PARTKEY FROM part WHERE P_TYPE = 'SMALL PLATED COPPER'"
    cursor.execute(query_part)
    part_keys = [row[0] for row in cursor.fetchall()]
mysql_conn.close()

# MongoDB connection and query
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
lineitem_cursor = mongodb['lineitem'].find(
    {"$and": 
        [
            {"L_PARTKEY": {"$in": part_keys}},
            {"L_SHIPDATE": {"$regex": "^199[56]"}},
            {"L_RETURNFLAG": "R"}  # Assuming 'R' flag indicates revenue
        ]
    },
    {"L_EXTENDEDPRICE": 1, "L_DISCOUNT": 1, "L_SUPPKEY": 1, "L_SHIPDATE": 1}
)
lineitems = pd.DataFrame(list(lineitem_cursor))
mongo_client.close()

# Redis connection and query
redis_conn = DirectRedis(host='redis', port=6379, db=0)
df_nation = pd.read_json(redis_conn.get('nation'))
df_supplier = pd.read_json(redis_conn.get('supplier'))
df_region = pd.read_json(redis_conn.get('region'))

# Filtering data for ASIA region and INDIA nation
asia_region_key = df_region[df_region['R_NAME'] == 'ASIA']['R_REGIONKEY'].iloc[0]
india_nation_key = df_nation[(df_nation['N_NAME'] == 'INDIA') & (df_nation['N_REGIONKEY'] == asia_region_key)]['N_NATIONKEY'].iloc[0]
india_suppliers = df_supplier[df_supplier['S_NATIONKEY'] == india_nation_key]['S_SUPPKEY']

# Combine lineitems with relevant supplier keys
lineitems['revenue'] = lineitems['L_EXTENDEDPRICE'] * (1 - lineitems['L_DISCOUNT'])
lineitems = lineitems[lineitems['L_SUPPKEY'].isin(india_suppliers)]
lineitems['year'] = lineitems['L_SHIPDATE'].str[:4]

# Calculate market share
market_share = lineitems.groupby('year')['revenue'].sum()

# Writing outcomes to CSV file
market_share.to_csv('query_output.csv', header=['Market Share'])
