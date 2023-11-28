import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_con = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')

# Connect to MongoDB
mongodb_con = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_con['tpch']

# Connect to Redis
redis_con = DirectRedis(host='redis', port=6379, db=0)

# Retrieve the 'part' table data from MySQL
sql_query = """
SELECT p.P_PARTKEY, p.P_NAME, p.P_MFGR, p.P_TYPE, p.P_SIZE
FROM part p
WHERE p.P_SIZE = 15 AND p.P_TYPE LIKE '%BRASS'
"""
parts_df_mysql = pd.read_sql(sql_query, mysql_con)

# Retrieve the 'nation' and 'region' table data from MongoDB
nation_docs = mongodb_db['nation'].find()
region_docs = mongodb_db['region'].find({'R_NAME': 'EUROPE'})

nation_df_mongo = pd.DataFrame(list(nation_docs))
region_df_mongo = pd.DataFrame(list(region_docs))

europe_nations = nation_df_mongo[nation_df_mongo['N_REGIONKEY'].isin(region_df_mongo['R_REGIONKEY'])]

# Retrieve the 'partsupp' table data from Redis
partsupp_df_redis = pd.read_json(redis_con.get('partsupp'), orient='records')

# Filter the suppliers in the EUROPE region
suppliers_df_mongo = pd.DataFrame(list(mongodb_db['supplier'].find()))
europe_suppliers = suppliers_df_mongo[suppliers_df_mongo['S_NATIONKEY'].isin(europe_nations['N_NATIONKEY'])]

europe_partsupp_df = partsupp_df_redis[partsupp_df_redis['PS_PARTKEY'].isin(parts_df_mysql['P_PARTKEY']) &
                                       partsupp_df_redis['PS_SUPPKEY'].isin(europe_suppliers['S_SUPPKEY'])]

# Merge the dataframe to get the required details and minimum supply cost
result_df = (parts_df_mysql.merge(europe_partsupp_df, how='inner', left_on='P_PARTKEY',
                                  right_on='PS_PARTKEY')
             .merge(europe_suppliers, how='inner', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
             .merge(europe_nations, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY'))

# Sorting results
sorted_result_df = result_df.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'],
                                         ascending=[False, True, True, True])

# Selecting required columns
selected_columns = [
    'S_ACCTBAL', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT',
    'P_PARTKEY', 'P_MFGR', 'P_SIZE', 'N_NAME'
]
final_result_df = sorted_result_df[selected_columns]

# Write results to CSV
final_result_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_con.close()
mongodb_con.close()
redis_con.close()
