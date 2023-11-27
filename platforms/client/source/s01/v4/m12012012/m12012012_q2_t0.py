# query.py
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connecting to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cur = mysql_conn.cursor()

# Fetch data from MySQL (part table)
mysql_cur.execute("SELECT * FROM part WHERE P_SIZE = 15 AND P_TYPE LIKE '%BRASS'")
parts = pd.DataFrame(mysql_cur.fetchall(), columns=['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'])

# Connecting to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Fetch data from MongoDB (nation and supplier tables)
nations = pd.DataFrame(list(mongo_db.nation.find()))
suppliers = pd.DataFrame(list(mongo_db.supplier.find()))

# Connecting to Redis
redis_conn = DirectRedis()

# Fetch data from Redis (region and partsupp tables)
regions = pd.read_json(redis_conn.get('region').decode('utf-8'))
partsupps = pd.read_json(redis_conn.get('partsupp').decode('utf-8'))

# Transforming MongoDB fields to match the SQL query
suppliers.rename(columns={'S_SUPPKEY': 'PS_SUPPKEY'}, inplace=True)
nations.rename(columns={'N_NATIONKEY': 'S_NATIONKEY'}, inplace=True)

# Filtering the regions for 'EUROPE' only
regions = regions[regions['R_NAME'] == 'EUROPE']

# Merging tables
merged_df = partsupp.merge(suppliers, on='PS_SUPPKEY', how='inner') \
    .merge(parts, on='P_PARTKEY', how='inner') \
    .merge(nations, on='S_NATIONKEY', how='inner') \
    .merge(regions, left_on='N_REGIONKEY', right_on='R_REGIONKEY', how='inner')

# Calculating minimum supply cost for EUROPE
min_supply_cost = merged_df.groupby(['P_PARTKEY'])['PS_SUPPLYCOST'].min().reset_index()

# Filter for the minimum PS_SUPPLYCOST and the rest of the conditions
final_df = merged_df.merge(min_supply_cost, on=['P_PARTKEY', 'PS_SUPPLYCOST'], how='inner')

# Order the final DataFrame
final_df.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True], inplace=True)

# Write results to query_output.csv
final_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_cur.close()
mysql_conn.close()
