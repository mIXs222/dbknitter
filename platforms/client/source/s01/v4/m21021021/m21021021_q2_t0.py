import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Establish a MySQL connection
mysql_conn = pymysql.connect(
    host='mysql', 
    user='root', 
    password='my-secret-pw', 
    database='tpch'
)

# Establish a MongoDB connection
mongo_client = pymongo.MongoClient(
    host='mongodb',
    port=27017
)
mongo_db = mongo_client['tpch']

# Retrieve MySQL data
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT P_PARTKEY, P_MFGR, P_TYPE, P_SIZE
        FROM part
        WHERE P_TYPE LIKE '%BRASS' AND P_SIZE = 15
    """)
    part_data = cursor.fetchall()

# Convert MySQL data to DataFrame
cols = ['P_PARTKEY', 'P_MFGR', 'P_TYPE', 'P_SIZE']
df_part = pd.DataFrame(part_data, columns=cols)

# Retrieve MongoDB data
region_data = list(mongo_db.region.find({'R_NAME': 'EUROPE'}, {'_id': 0}))
partsupp_data = list(mongo_db.partsupp.find({}, {'_id': 0}))

# Convert MongoDB data to DataFrame
df_region = pd.DataFrame(region_data)
df_partsupp = pd.DataFrame(partsupp_data)

# Retrieve Redis data
redis_conn = DirectRedis(host='redis', port=6379, db=0)
df_nation = pd.read_json(redis_conn.get('nation'))
df_supplier = pd.read_json(redis_conn.get('supplier'))

# Perform filtering to simulate SQL subquery on partsupp and region
subquery_df = df_partsupp.merge(df_supplier, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
subquery_df = subquery_df.merge(df_nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
subquery_df = subquery_df.merge(df_region, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
min_cost = subquery_df['PS_SUPPLYCOST'].min()

# Merge dataframes to simulate join operations
df_merged = df_part.merge(df_partsupp[df_partsupp.PS_SUPPLYCOST == min_cost], left_on='P_PARTKEY', right_on='PS_PARTKEY')
df_merged = df_merged.merge(df_supplier, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
df_merged = df_merged.merge(df_nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
df_merged = df_merged.merge(df_region, left_on='N_REGIONKEY', right_on='R_REGIONKEY')

# Select and order the results
df_result = df_merged[[
    'S_ACCTBAL',
    'S_NAME',
    'N_NAME',
    'P_PARTKEY',
    'P_MFGR',
    'S_ADDRESS',
    'S_PHONE',
    'S_COMMENT'
]].sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

# Write result to file
df_result.to_csv('query_output.csv', index=False)
